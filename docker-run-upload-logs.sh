#!/bin/bash

set -e

IMAGE_NAME=ahadi

# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: ./docker-run-upload-logs.sh
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <memory-profile-path>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
RUN_ID=$4
INPUT_MEMORY_PROFILE=$5

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u upload_logs.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \
    \"$RUN_ID\" /data/memory.profile
"
container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_MEMORY_PROFILE" "$container:/data/memory.profile"

# Run the container
docker start -a -i "$container"

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
