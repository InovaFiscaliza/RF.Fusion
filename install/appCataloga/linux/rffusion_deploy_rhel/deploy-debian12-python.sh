#!/bin/bash
set -e
echo -e "\e[32m=== Deploying Debian 12 Python container (Linux native) ===\e[0m"

echo -e "\e[36m\n--- STEP 1: Podman setup ---\e[0m"
bash "$(dirname "$0")/setup-podman.sh"

echo -e "\e[36m\n--- STEP 2: Network setup ---\e[0m"
bash "$(dirname "$0")/setup-network.sh"

echo -e "\e[36m\n--- STEP 3: Build image ---\e[0m"
bash "$(dirname "$0")/build-debian12-python.sh"

echo -e "\e[36m\n--- STEP 4: Run container ---\e[0m"
bash "$(dirname "$0")/run-debian12-python.sh"

echo -e "\e[32m\n=== Deployment finished successfully ===\e[0m"
