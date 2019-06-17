#!/usr/bin/env bash

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: ./5_backup_data_root <data-root> <data-backups-dir>"
    echo "Backs-up the data root directory to a compressed file in a backups directory"
    echo "The directory is gzipped and given the name 'data-<utc-date-time-now>-<git-HEAD-hash>'"
    exit
fi

DATA_ROOT=$1
DATA_BACKUPS_DIR=$2

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
mkdir -p "$DATA_BACKUPS_DIR"
find "$DATA_ROOT" -type f -name '.DS_Store' -delete
cd "$DATA_ROOT"
tar -czvf "$DATA_BACKUPS_DIR/data-$DATE-$HASH.tar.gzip" .
