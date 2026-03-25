#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_LOCATION:=us-east1}"
: "${SERVICE_NAME:=rwa-demo-api}"

IMAGE="gcr.io/${GOOGLE_CLOUD_PROJECT}/${SERVICE_NAME}"

gcloud builds submit . \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --config cloudbuild-api.yaml \
  --substitutions "_IMAGE=${IMAGE},_GCS_POLICY_BUCKET=${GCS_POLICY_BUCKET}"

gcloud run deploy "${SERVICE_NAME}" \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_LOCATION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --set-env-vars "GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT},GOOGLE_CLOUD_LOCATION=${GOOGLE_CLOUD_LOCATION},BIGQUERY_DATASET=${BIGQUERY_DATASET:-finance_demo},GCS_POLICY_BUCKET=${GCS_POLICY_BUCKET:?Set GCS_POLICY_BUCKET},USE_VERTEX_AGENT=true,VERTEX_MODEL=${VERTEX_MODEL:-gemini-2.5-pro},DEMO_MODE=cloud"

echo "API deployed: ${SERVICE_NAME}"
