#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./5_backup_data_root <data-root> <data-backups-dir> <run-id>"
    echo "Backs-up the data root directory to a compressed file in a backups directory"
    echo "The directory is gzipped and given the name 'data-<run-id>-<git-HEAD-hash>'"
    exit
fi

DATA_ROOT=$1
DATA_BACKUPS_DIR=$2
RUN_ID=$3

HASH=$(git rev-parse HEAD)
mkdir -p "$DATA_BACKUPS_DIR"
find "$DATA_ROOT" -type f -name '.DS_Store' -delete
cd "$DATA_ROOT"
tar -czvf "$DATA_BACKUPS_DIR/data-$RUN_ID-$HASH.tar.gzip" .
