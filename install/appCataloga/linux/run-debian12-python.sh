#!/bin/bash
set -e

echo -e "\e[32m=== Running container debian12-python ===\e[0m"

CONTAINER_NAME="debian12-python"
NETWORK="rffusion-net"
SSH_PORT=2222
APP_PORT=5555
HOST_VOLUME_RFF="/RFFusion-dev/RF.Fusion"
HOST_VOLUME_REPOS="/mnt/reposfi"
CONTAINER_VOLUME_RFF="/RFFusion"
CONTAINER_VOLUME_REPOS="/mnt/reposfi"

# --- Verifica se a rede existe ---
if ! podman network exists "$NETWORK"; then
    echo -e "\e[33mNetwork $NETWORK not found. Creating it...\e[0m"
    podman network create "$NETWORK" --subnet=10.99.0.0/24 --gateway=10.99.0.1
else
    echo -e "\e[36mUsing existing network: $NETWORK\e[0m"
fi

# --- Remove container antigo se existir ---
if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "\e[33mContainer $CONTAINER_NAME already exists. Removing...\e[0m"
    podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# --- Detecta se o volume suporta atributos estendidos (xattr) ---
LABEL_OPT=":Z"
TMP_FILE="${HOST_VOLUME_REPOS}/.xattr_test"
if ! (touch "$TMP_FILE" 2>/dev/null && setfattr -n user.test -v test "$TMP_FILE" 2>/dev/null); then
    LABEL_OPT=":rw"
    echo -e "\e[33mWarning: Volume $HOST_VOLUME_REPOS does not support xattr. Using $LABEL_OPT instead of :Z.\e[0m"
fi
rm -f "$TMP_FILE" 2>/dev/null || true

# --- Executa o container ---
echo -e "\e[36mStarting container $CONTAINER_NAME on network $NETWORK...\e[0m"
podman run -d \
  --name "$CONTAINER_NAME" \
  --hostname "$CONTAINER_NAME" \
  --network "$NETWORK" \
  -p ${SSH_PORT}:22 \
  -p ${APP_PORT}:5555 \
  -e SSH_PASSWORD=changeme \
  -v "${HOST_VOLUME_RFF}:${CONTAINER_VOLUME_RFF}:Z" \
  -v "${HOST_VOLUME_REPOS}:${CONTAINER_VOLUME_REPOS}${LABEL_OPT}" \
  debian12-python

echo -e "\e[32mContainer started successfully!\e[0m"
podman ps

echo -e "\n\e[36mNetwork details for $CONTAINER_NAME:\e[0m"
podman port "$CONTAINER_NAME"
echo -e "\nContainer IP:"
podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CONTAINER_NAME"

echo -e "\e[36m\nMounted host paths:\e[0m"
echo -e "  ${HOST_VOLUME_RFF}  →  ${CONTAINER_VOLUME_RFF}"
echo -e "  ${HOST_VOLUME_REPOS}  →  ${CONTAINER_VOLUME_REPOS}"
