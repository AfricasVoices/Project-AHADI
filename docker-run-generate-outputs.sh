#!/bin/bash

set -e

IMAGE_NAME=ahadi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --profile-memory)
            PROFILE_MEMORY=true
            MEMORY_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 12 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>] [--profile-memory <profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path>
    <raw-data-dir> <prev-coded-dir> <messages-json-output-path> <individuals-json-output-path>
    <icr-output-dir> <coded-output-dir> <messages-output-csv> <individuals-output-csv> <production-output-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
INPUT_RAW_DATA_DIR=$4
PREV_CODED_DIR=$5
OUTPUT_MESSAGES_JSON=$6
OUTPUT_INDIVIDUALS_JSON=$7
OUTPUT_ICR_DIR=$8
OUTPUT_CODED_DIR=$9
OUTPUT_MESSAGES_CSV=${10}
OUTPUT_INDIVIDUALS_CSV=${11}
OUTPUT_PRODUCTION_CSV=${12}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" --build-arg INSTALL_MEMORY_PROFILER="$PROFILE_MEMORY" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
if [[ "$PROFILE_MEMORY" = true ]]; then
    PROFILE_MEMORY_CMD="mprof run -o /data/memory.prof"
fi
CMD="pipenv run $PROFILE_CPU_CMD $PROFILE_MEMORY_CMD python -u generate_outputs.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \
    /data/raw-data /data/prev-coded \
    /data/output-messages.json /data/output-individuals.json /data/output-icr /data/coded \
    /data/output-messages.csv /data/output-individuals.csv /data/output-production.csv
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_RAW_DATA_DIR" "$container:/data/raw-data"
if [[ -d "$PREV_CODED_DIR" ]]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_MESSAGES_JSON")"
docker cp "$container:/data/output-messages.json" "$OUTPUT_MESSAGES_JSON"

mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_JSON")"
docker cp "$container:/data/output-individuals.json" "$OUTPUT_INDIVIDUALS_JSON"

mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

mkdir -p "$OUTPUT_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_CODED_DIR"

mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

if [[ "$PROFILE_MEMORY" = true ]]; then
    mkdir -p "$(dirname "$MEMORY_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/memory.prof" "$MEMORY_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
