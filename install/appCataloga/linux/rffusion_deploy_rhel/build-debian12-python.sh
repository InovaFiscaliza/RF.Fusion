#!/bin/bash
set -e
echo -e "\e[32m=== Building Debian 12 Python image ===\e[0m"
podman build -t rffusion-debian12-python -f "$(dirname "$0")/Containerfile"
