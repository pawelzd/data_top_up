#!/usr/bin/env bash
# hourly_ingest.sh — pull the last closed hour of OHLCV for the tradable universe
# into core.token_ohlcv (Cloud Run Job, runs every hour). Thin wrapper over
# backfill_birdeye_ohlcv.py, which is idempotent: it dedups against existing
# (token, hour) rows, so re-pulling the last few hours is safe. --end-date
# defaults to the current UTC hour (the in-progress candle is skipped), so each
# run lands the most recent CLOSED hour.
#
# Auth: BIRDEYE_API_KEY (Secret Manager) + ADC for BigQuery (the runtime SA).
set -euo pipefail
cd "$(dirname "$0")"

# Unified structured-log line (same envelope as deploy/obs_log.py) via python3.
OBS_SERVICE="${OBS_SERVICE:-birdeye-hourly-ingest}"
log_json() {  # log_json EVENT LEVEL [key=value ...]
  OBS_SERVICE="$OBS_SERVICE" python3 - "$@" <<'PY'
import sys, os, json, datetime
ev = sys.argv[1] if len(sys.argv) > 1 else "event"
level = sys.argv[2] if len(sys.argv) > 2 else "INFO"
rec = {"ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
       "service": os.environ.get("OBS_SERVICE", "job"), "event": ev, "level": level}
for kv in sys.argv[3:]:
    k, _, v = kv.partition("=")
    try:
        v = json.loads(v)
    except Exception:
        pass
    rec[k] = v
print(json.dumps(rec, separators=(",", ":")))
PY
}

HOURS_BACK="${HOURS_BACK:-3}"                 # small look-back covers a missed run
TABLE="${BIGQUERY_TABLE:-crypto-trading-474111.core.token_ohlcv}"
# The tradable set the decision service reads. Override for a narrower/cheaper set.
TOKENS_QUERY="${TOKENS_QUERY:-SELECT DISTINCT token_address FROM \`crypto-trading-474111.rl_prod.rl_prod_inference_features_v\`}"

START="$(python3 - "$HOURS_BACK" <<'PY'
import sys, datetime as dt
hb = int(sys.argv[1])
print((dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hb)).strftime("%Y-%m-%dT%H:00:00"))
PY
)"

log_json cycle_start INFO "start=${START}" "table=${TABLE}" "hours_back=${HOURS_BACK}"
# Run (not exec) so we can emit a structured cycle_done/cycle_error afterwards.
# `set -e` would abort on non-zero, so capture rc explicitly.
if python3 backfill_birdeye_ohlcv.py \
  --start-date "${START}" \
  --table "${TABLE}" \
  --tokens-query "${TOKENS_QUERY}" \
  --bigquery-location "${BIGQUERY_LOCATION:-europe-central2}" \
  --rate-limit-rpm "${RATE_LIMIT_RPM:-100}" \
  --flush-row-threshold "${FLUSH_ROW_THRESHOLD:-1000}" \
  ${EXTRA_ARGS:-}; then rc=0; else rc=$?; fi

if [ "$rc" -eq 0 ]; then
  log_json cycle_done INFO "rc=0"
else
  log_json cycle_error ERROR "rc=${rc}"
fi
exit "$rc"
