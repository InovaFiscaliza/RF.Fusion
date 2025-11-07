#!/bin/bash
set -e
echo -e "\e[32m=== Deploying Debian 12 Python container (Linux native) ===\e[0m"

echo -e "\n--- STEP 1: Podman setup ---"
bash setup-podman.sh

echo -e "\n--- STEP 2: Network setup ---"
bash setup-network.sh

echo -e "\n--- STEP 3: Build image ---"
bash build-debian12-python.sh

echo -e "\n--- STEP 4: Run container ---"
bash run-debian12-python.sh

echo -e "\e[32m\n=== Deployment finished successfully ===\e[0m"
