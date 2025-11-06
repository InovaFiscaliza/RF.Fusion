#!/bin/bash
set -e
echo -e "\e[32m=== Building image debian12-python ===\e[0m"
podman build -t debian12-python -f Containerfile .
echo -e "\e[32mBuild complete.\e[0m"
