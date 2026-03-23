#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_LOCATION:=us-central1}"
: "${SERVICE_NAME:=rwa-demo-ui}"

gcloud run deploy "${SERVICE_NAME}" \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_LOCATION}" \
  --source . \
  --dockerfile Dockerfile.ui \
  --allow-unauthenticated

echo "UI deployed: ${SERVICE_NAME}"

