#!/usr/bin/env bash
# deploy_candidate_pull.sh — build + deploy the birdeye-candidate-pull Cloud Run
# JOB (weekly universe discovery, §2.3). The weekly Cloud Workflow (in rl-crypto)
# invokes it as the first step of the chain.
#
# Runtime SA needs BigQuery read/write on `raw` (the snapshot) and read on
# `core.token_ohlcv` (new-token detection) + write if OHLCV backfill is on.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-europe-central2}"
REPO="${REPO:-rl}"                                    # Artifact Registry repo
JOB="${JOB:-birdeye-candidate-pull}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${JOB}:latest}"
RUNTIME_SA="${RUNTIME_SA:-rl-ingest@${PROJECT}.iam.gserviceaccount.com}"

# Candidate screen (deliberately loose — the membership rule tightens downstream).
MAX_TOKENS="${MAX_TOKENS:-500}"
MIN_LIQUIDITY_USD="${MIN_LIQUIDITY_USD:-50000}"
SORT_BY="${SORT_BY:-volume_24h_usd}"
BACKFILL_NEW_TOKENS="${BACKFILL_NEW_TOKENS:-true}"

echo "[deploy] building $IMAGE"
docker build -f Dockerfile.candidate_pull -t "$IMAGE" .
docker push "$IMAGE"

gcloud run jobs deploy "$JOB" \
  --project "$PROJECT" --region "$REGION" \
  --image "$IMAGE" --service-account "$RUNTIME_SA" \
  --max-retries 1 --task-timeout 3600 \
  --set-env-vars "BQ_PROJECT_ID=${PROJECT},MARKET_DATA_TABLE=${PROJECT}.raw.raw_birdeye_market_data,OHLCV_TABLE=${PROJECT}.core.token_ohlcv,BIGQUERY_LOCATION=europe-central2,MAX_TOKENS=${MAX_TOKENS},MIN_LIQUIDITY_USD=${MIN_LIQUIDITY_USD},SORT_BY=${SORT_BY},BACKFILL_NEW_TOKENS=${BACKFILL_NEW_TOKENS}" \
  --set-secrets "BIRDEYE_API_KEY=birdeye-api-key:latest"

echo "[deploy] done. Job $JOB deployed. The universe-weekly Workflow (rl-crypto)"
echo "         runs it first, before universe-membership-advance."
