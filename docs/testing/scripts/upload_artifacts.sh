#!/usr/bin/env bash
set -euo pipefail

if [[ "${AWS_S3_BUCKET:-}" = "" ]]; then
  echo "ERROR: AWS_S3_BUCKET is not set" >&2
  exit 1
fi

if [[ "${ARTIFACT_PATH:-}" = "" ]]; then
  echo "ERROR: ARTIFACT_PATH is not set" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found on PATH" >&2
  exit 1
fi

run_id="${CI_RUN_ID:-manual.$(date +%Y%m%d_%H%M%S)}"
dest="s3://${AWS_S3_BUCKET%/}/python-api/${run_id}/"

echo "Uploading ${ARTIFACT_PATH} to ${dest}"
aws s3 cp "${ARTIFACT_PATH}" "${dest}" --recursive --only-show-errors
echo "Upload complete."
