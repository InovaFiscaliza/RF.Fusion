#!/bin/bash
set -e
echo -e "\e[32m=== [1/3] Checking Podman installation ===\e[0m"
if ! command -v podman >/dev/null 2>&1; then
    echo -e "\e[33mPodman not found. Installing...\e[0m"
    dnf install -y podman
else
    echo -e "\e[36mPodman is already installed.\e[0m"
fi

echo -e "\n\e[32m=== [2/3] Checking Podman group permissions ===\e[0m"
if getent group podman >/dev/null 2>&1; then
    echo -e "\e[36mPodman group exists.\e[0m"
else
    echo -e "\e[33mNo 'podman' group found. (Normal in native RHEL installs)\e[0m"
fi

echo -e "\n\e[32m=== [3/3] Validating Podman environment ===\e[0m"
if podman info >/dev/null 2>&1; then
    echo -e "\e[36mPodman is functional.\e[0m"
else
    echo -e "\e[31mPodman is not functional. Check installation.\e[0m"
    exit 1
fi
echo -e "\e[32mPodman setup complete.\e[0m"
