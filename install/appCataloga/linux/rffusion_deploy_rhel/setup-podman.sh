#!/bin/bash
set -e

echo -e "\e[32m=== [1/3] Checking Podman installation ===\e[0m"

# Verifica se o Podman está instalado
if ! command -v podman &>/dev/null; then
  echo "Podman not found, installing..."
  if command -v dnf &>/dev/null; then
    sudo dnf install -y podman
  elif command -v yum &>/dev/null; then
    sudo yum install -y podman
  else
    echo "ERROR: Package manager not found (dnf/yum). Please install Podman manually."
    exit 1
  fi
else
  echo "Podman is already installed."
fi

echo -e "\e[32m=== [2/3] Checking Podman group permissions ===\e[0m"

# Verifica se o grupo "podman" existe
if getent group podman >/dev/null; then
  echo "Podman group exists."
  # Só tenta adicionar o usuário se não for root
  if [ "$EUID" -ne 0 ]; then
    echo "Adding user $USER to podman group..."
    sudo usermod -aG podman "$USER"
  else
    echo "Running as root, skipping usermod."
  fi
else
  echo "Podman group not found — this is normal on RHEL native installs."
  echo "Skipping group assignment."
fi

echo -e "\e[32m=== [3/3] Validating Podman environment ===\e[0m"

# Testa se o Podman está funcional
if ! podman info &>/dev/null; then
  echo "ERROR: Podman installation seems broken or not configured properly."
  echo "Try running 'sudo podman system migrate' or reinstalling Podman."
  exit 1
else
  echo "Podman is functional."
fi

echo -e "\e[32mPodman setup complete.\e[0m"
