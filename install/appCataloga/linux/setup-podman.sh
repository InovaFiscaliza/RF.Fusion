#!/bin/bash
set -e

echo -e "\e[32m=== [1/3] Checking Podman installation ===\e[0m"

# Verifica se o Podman está instalado
if ! command -v podman &>/dev/null; then
  echo "Podman not found. Installing..."
  if command -v dnf &>/dev/null; then
    sudo dnf install -y podman
  elif command -v yum &>/dev/null; then
    sudo yum install -y podman
  else
    echo "ERROR: No supported package manager found (dnf/yum)."
    exit 1
  fi
else
  echo "Podman is already installed."
fi

echo -e "\e[32m=== [2/3] Checking Podman group permissions ===\e[0m"

# Detecta se grupo 'podman' existe
if getent group podman >/dev/null; then
  echo "Podman group found."
  if [ "$EUID" -ne 0 ]; then
    echo "Adding user $USER to podman group..."
    sudo usermod -aG podman "$USER"
  else
    echo "Running as root — no need to add to podman group."
  fi
else
  echo "No 'podman' group found. (Normal in native RHEL installs)"
  echo "Skipping group assignment step."
fi

echo -e "\e[32m=== [3/3] Validating Podman environment ===\e[0m"

# Testa se Podman responde corretamente
if podman info &>/dev/null; then
  echo "Podman is functional."
else
  echo "ERROR: Podman installation check failed."
  echo "Try running 'podman system migrate' or reinstall Podman."
  exit 1
fi

echo -e "\e[32mPodman setup complete.\e[0m"
