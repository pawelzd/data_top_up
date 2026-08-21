#!/usr/bin/env bash
# deploy_weekly_wide_backfill.sh — the WEEKLY WIDE OHLCV pull that keeps universe
# discovery alive while hourly ingestion stays narrow and cheap.
#
# WHY THIS JOB EXISTS
# -------------------
# Hourly ingestion is deliberately scoped to the ~100 tokens the book can trade;
# pulling ~1,400 tokens every hour was too expensive and was cut on 2026-07-21.
# That cut had a side effect nobody chose: membership entry requires
# `trailing_bar_count >= 500` (~21 days of hourly bars), but only tokens already
# in the universe were getting bars. A token could not enter without history, and
# could not get history without entering. Measured 2026-08-21: tokens carrying a
# volume rank fell 1,509 -> 454, dollar volume at rank #120 fell $113,873 -> $2,991,
# and 3,022 of 3,402 discovered candidates had no bar in the last 7 days.
#
# The fix is cadence, not breadth-at-any-price. Birdeye v1 /defi/ohlcv is a FLAT
# 35 CU per call and returns up to 1,000 candles, so ONE call buys a token its
# entire 30-day trailing window. Discovery therefore does not need to run hourly —
# it needs to run once a week, right before membership is computed.
#
#   ~999 tokens x 1 call x 35 CU  =  ~35k CU/week  (~5k CU/day)
#   versus the old always-wide hourly ingest at ~1.18M CU/day.
#
# Because HOURS_BACK=720 overlaps the 168 hours since the previous run, the record
# for non-members is CONTINUOUS, just batch-written. Two consequences worth naming:
#   * A token promoted on Monday ALREADY has 720 trailing hourly bars, so its 168h
#     features are computable on day one and the hourly job simply continues the
#     series. No warm-up gap, and no change to any membership rule.
#   * A token that EXITS keeps weekly coverage, so rugs and fades stay in the
#     record. Ingesting only survivors re-accumulates the survivorship bias the
#     PIT rebuild was done to remove.
#
# TOKENS_QUERY IS IN THIS FILE ON PURPOSE
# ---------------------------------------
# The hourly job's breadth was configured out-of-band and then silently lost TWICE
# to `--set-env-vars` (which replaces the whole environment) — once around
# 2026-07-21 and again after the 2026-08-10 repair. Nothing alerted either time.
# Every input to this job is declared here, so redeploying it is idempotent and a
# reviewer can see the breadth in the diff.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-europe-central2}"
REPO="${REPO:-rl}"
JOB="${JOB:-birdeye-weekly-wide-backfill}"
# Same IMAGE as birdeye-hourly-ingest: identical, already-exercised code. This job
# is a different SCHEDULE and SCOPE, not different behaviour.
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/birdeye-hourly-ingest:merge-20260814-1349}"
RUNTIME_SA="${RUNTIME_SA:-rl-prod@${PROJECT}.iam.gserviceaccount.com}"

# 720h = 30 days = the exact window the membership rules read
# (trailing_bar_count, median_mktcap_30d, dollar_volume_rank_30d). Under v1's
# 1,000-candle cap this stays a single call per token. Raising it above 1000
# silently truncates instead of erroring — v1 caps the window at 1000h.
HOURS_BACK="${HOURS_BACK:-720}"

# Three legs, deliberately a strict SUPERSET of what the hourly job pulls:
#   serving       — never let a tradable token lose coverage
#   discovered    — this week's discovery pull; the entry pipeline
#   still_trading — anything already tracked that still trades, which is what
#                   keeps the death tail in the record
# The address guard also excludes the `btcusdt` reference row (7 chars), which is
# written by a different process and must not be re-pulled from Birdeye.
read -r -d '' TOKENS_QUERY <<'SQL' || true
WITH serving AS (
  SELECT DISTINCT token_address
  FROM `crypto-trading-474111.rl_prod.rl_prod_inference_features_v`
),
discovered AS (
  SELECT DISTINCT token_address
  FROM `crypto-trading-474111.raw.candidate_universe`
  WHERE chain = 'sol'
    AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
),
still_trading AS (
  SELECT token_address
  FROM `crypto-trading-474111.core.token_ohlcv`
  WHERE chain = 'sol'
    AND price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY token_address
  HAVING SUM(volume * close) >= 200000
)
SELECT token_address
FROM (
  SELECT token_address FROM serving
  UNION DISTINCT SELECT token_address FROM discovered
  UNION DISTINCT SELECT token_address FROM still_trading
)
WHERE LENGTH(token_address) BETWEEN 32 AND 44
  AND REGEXP_CONTAINS(token_address, r'^[1-9A-HJ-NP-Za-km-z]+$')
SQL

echo "[deploy] job=$JOB image=$IMAGE hours_back=$HOURS_BACK"

# ^@^ delimiter: the query contains commas, which is the default separator.
gcloud run jobs deploy "$JOB" \
  --project "$PROJECT" --region "$REGION" \
  --image "$IMAGE" --service-account "$RUNTIME_SA" \
  --max-retries 1 --task-timeout 3600 --cpu 1 --memory 2Gi \
  --set-env-vars "^@^BIGQUERY_TABLE=${PROJECT}.core.token_ohlcv@BIGQUERY_LOCATION=${REGION}@HOURS_BACK=${HOURS_BACK}@RATE_LIMIT_RPM=${RATE_LIMIT_RPM:-800}@OHLCV_API=v1@FLUSH_ROW_THRESHOLD=${FLUSH_ROW_THRESHOLD:-50000}@OBS_SERVICE=${JOB}@TOKENS_QUERY=${TOKENS_QUERY}" \
  --set-secrets "BIRDEYE_API_KEY=birdeye-api-key:latest"

echo "[deploy] verifying TOKENS_QUERY landed (breadth is the whole point of this job)"
_got="$(gcloud run jobs describe "$JOB" --project "$PROJECT" --region "$REGION" --format=json \
  | python3 -c 'import json,sys
d=json.load(sys.stdin)
env=d["spec"]["template"]["spec"]["template"]["spec"]["containers"][0].get("env",[])
print(next((e.get("value","") for e in env if e["name"]=="TOKENS_QUERY"), ""))')"
case "$_got" in
  *candidate_universe*still_trading*|*still_trading*)
    echo "[deploy] OK: wide TOKENS_QUERY is set on $JOB" ;;
  "") echo "[deploy] FAILED: TOKENS_QUERY is EMPTY — the job would fall back to the" >&2
      echo "[deploy] narrow hourly default and discovery would stay dead." >&2; exit 1 ;;
  *)  echo "[deploy] FAILED: TOKENS_QUERY is not the wide query. Got: ${_got:0:120}..." >&2; exit 1 ;;
esac

echo "[deploy] done. NOT scheduled directly — the universe-weekly Workflow runs it"
echo "         between the candidate pull and membership_advance, so membership is"
echo "         always computed on freshly-backfilled bars."
