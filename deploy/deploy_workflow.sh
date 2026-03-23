#!/usr/bin/env bash
set -euo pipefail

: "${GOOGLE_CLOUD_PROJECT:?Set GOOGLE_CLOUD_PROJECT}"
: "${GOOGLE_CLOUD_LOCATION:=us-central1}"
: "${WORKFLOW_NAME:=rwa-policy-pipeline}"

gcloud workflows deploy "${WORKFLOW_NAME}" \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --location "${GOOGLE_CLOUD_LOCATION}" \
  --source workflows/rwa_policy_pipeline.yaml

echo "Workflow deployed: ${WORKFLOW_NAME}"

