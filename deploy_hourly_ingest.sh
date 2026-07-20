#!/usr/bin/env bash
# deploy_hourly_ingest.sh — build+deploy the birdeye-hourly-ingest Cloud Run JOB
# and its hourly Scheduler trigger (:02 past the hour, first in the hourly chain:
# ingest :02 -> dbt transform :08 -> shadow decide :12).
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-rl}"
JOB="${JOB:-birdeye-hourly-ingest}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${JOB}:latest}"
RUNTIME_SA="${RUNTIME_SA:-rl-ingest@${PROJECT}.iam.gserviceaccount.com}"
SCHEDULER_SA="${SCHEDULER_SA:-rl-scheduler@${PROJECT}.iam.gserviceaccount.com}"

echo "[deploy] building $IMAGE"
docker build -f Dockerfile.hourly_ingest -t "$IMAGE" .
docker push "$IMAGE"

gcloud run jobs deploy "$JOB" \
  --project "$PROJECT" --region "$REGION" \
  --image "$IMAGE" --service-account "$RUNTIME_SA" \
  --max-retries 1 --task-timeout 1800 \
  --set-env-vars "BIGQUERY_TABLE=${PROJECT}.core.token_ohlcv,BIGQUERY_LOCATION=europe-central2,HOURS_BACK=3,RATE_LIMIT_RPM=100" \
  --set-secrets "BIRDEYE_API_KEY=birdeye-api-key:latest"

# Hourly trigger at :02 — Cloud Scheduler -> Cloud Run Jobs API (jobs.run).
JOB_RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run"
gcloud scheduler jobs create http "${JOB}-hourly" \
  --project "$PROJECT" --location "$REGION" \
  --schedule "2 * * * *" --time-zone "Etc/UTC" \
  --uri "$JOB_RUN_URI" --http-method POST \
  --oauth-service-account-email "$SCHEDULER_SA" \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" \
  || gcloud scheduler jobs update http "${JOB}-hourly" \
       --project "$PROJECT" --location "$REGION" --schedule "2 * * * *" --uri "$JOB_RUN_URI"

echo "[deploy] done. birdeye-hourly-ingest runs at :02 -> core.token_ohlcv"
