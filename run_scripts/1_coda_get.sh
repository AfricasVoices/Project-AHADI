#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="AHADI"
DATASETS=(
    "s01e01"
    "s01e02"
    "s01e03"
    "s01e04"

    "location"
    "gender"
    "age"
    "livelihood"

    "understanding"
    "inclusion"
    "trust"
    "positive_changes"
)

cd "$CODA_V2_ROOT/data_tools"
mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    echo "Getting messages data from ${PROJECT_NAME}_${DATASET}..."

    pipenv run python get.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages >"$DATA_ROOT/Coded Coda Files/$DATASET.json"
done
