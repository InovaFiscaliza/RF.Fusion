#!/bin/bash

# Working folders and files
TARGET_FOLDER="/mnt/internal/data/"
SLEEP_TIME=60

# Ensure target folder exists
if [ ! -d "$TARGET_FOLDER" ]; then
    echo "Target folder $TARGET_FOLDER does not exist. Creating..."
    mkdir -p "$TARGET_FOLDER"
fi

declare -a TEST_FILES=(
    "/mnt/internal/SCAN_M_450470_rfeye002088_170426_162736.bin"
    "/mnt/internal/SCAN_M_450470_rfeye002088_170426_164029.bin"
    "/mnt/internal/SCAN_M_450470_rfeye002088_170426_165322.bin"
    "/mnt/internal/SCAN_M_450470_rfeye002088_170426_170615.bin"
)

for fileName in "${TEST_FILES[@]}"; do

    echo "Copying $fileName"

    cp "$fileName" "$TARGET_FOLDER"

    if [ $SLEEP_TIME -gt 60 ]; then
        echo "Waiting $(($SLEEP_TIME / 60)) minutes"
    else
        echo "Waiting $SLEEP_TIME seconds"
    fi

    sleep $SLEEP_TIME

done
