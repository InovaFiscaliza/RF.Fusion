#!/bin/bash

# Working folders and files
TARGET_FOLDER="/mnt/internal/data/"

declare -a TEST_FILES=(
    "/mnt/internal/test/SCAN_M_450470_rfeye002088_170426_162736.bin"
    "/mnt/internal/test/SCAN_M_450470_rfeye002088_170426_164029.bin"
    "/mnt/internal/test/SCAN_M_450470_rfeye002088_170426_165322.bin"
    "/mnt/internal/test/SCAN_M_450470_rfeye002088_170426_170615.bin"
)

for fileName in "${TEST_FILES[@]}"; do

    cp "$fileName" "$TARGET_FOLDER"

    echo "copied $fileName"

    sleep 20

    echo "finished waiting 20 seconds"
done
