#!/usr/bin/env bash

set -e

if [[ $# -ne 5 ]]; then
    echo "Usage: ./6_upload_logs <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <memory-profile-file-path>"
    echo "Uploads the pipeline logs"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
MEMORY_PROFILE_FILE_PATH=$5

cd ..
./docker-run-upload-logs.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$RUN_ID" "$MEMORY_PROFILE_FILE_PATH"
