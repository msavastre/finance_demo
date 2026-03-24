#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_LOCATION:=us-central1}"
: "${SERVICE_NAME:=rwa-demo-api}"

IMAGE="gcr.io/${GOOGLE_CLOUD_PROJECT}/${SERVICE_NAME}"

gcloud builds submit . \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --config cloudbuild-api.yaml \
  --substitutions "_IMAGE=${IMAGE}"

gcloud run deploy "${SERVICE_NAME}" \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_LOCATION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated

echo "API deployed: ${SERVICE_NAME}"
