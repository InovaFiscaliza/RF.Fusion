#!/bin/bash

# Working folders and files
TARGET_FOLDER="/mnt/internal/data/"

declare -a TEST_FILES=(
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_162736.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_182333.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_201930.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_221527.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_164029.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_183626.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_203223.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_222820.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_165322.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_184919.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_204516.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_224113.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_170615.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_190212.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_205809.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_225406.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_171908.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_191505.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_211102.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_230659.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_173201.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_192758.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_212355.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_231952.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_174454.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_194051.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_213648.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_233245.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_175747.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_195344.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_214941.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_235831.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_181040.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_200637.bin"
    "/mnt/internal/ref/SCAN_M_450470_rfeye002088_170426_220234.bin"
)

for fileName in "${TEST_FILES[@]}"; do

    # Default configuration is "subnet"
    cp "$fileName" "$TARGET_FOLDER"

    echo "copied $fileName"

    sleep 1800

done
