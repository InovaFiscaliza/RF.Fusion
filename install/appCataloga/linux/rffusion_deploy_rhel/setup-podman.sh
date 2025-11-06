#!/bin/bash
set -e
echo -e "\e[32m=== [1/3] Checking Podman installation ===\e[0m"
if ! command -v podman &>/dev/null; then
  echo "Podman not found, installing..."
  sudo dnf install -y podman
else
  echo "Podman is already installed."
fi

echo -e "\e[32m=== [2/3] Checking user group permissions ===\e[0m"
if ! groups | grep -q podman; then
  echo "Adding $USER to podman group..."
  sudo usermod -aG podman "$USER"
fi

echo -e "\e[32m=== [3/3] Podman setup complete ===\e[0m"
