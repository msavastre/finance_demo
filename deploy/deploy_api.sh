#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_LOCATION:=us-central1}"
: "${SERVICE_NAME:=rwa-demo-api}"

IMAGE="gcr.io/${GOOGLE_CLOUD_PROJECT}/${SERVICE_NAME}"

# Build image using the API Dockerfile
gcloud builds submit . \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --dockerfile Dockerfile.api \
  --tag "${IMAGE}"

# Deploy to Cloud Run from the built image
gcloud run deploy "${SERVICE_NAME}" \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_LOCATION}" \
  --image "${IMAGE}" \
  --allow-unauthenticated

echo "API deployed: ${SERVICE_NAME}"
