#!/bin/bash
set -e

echo -e "\e[32m=== Running container rffusion-debian12-python ===\e[0m"

CONTAINER_NAME="debian12-python"
NETWORK="rffusion-net"
SSH_PORT=2222
APP_PORT=5555

# --- Verifica se as portas estão livres ---
echo -e "\e[36mChecking if ports $SSH_PORT and $APP_PORT are available...\e[0m"
for PORT in $SSH_PORT $APP_PORT; do
    if ss -tuln | grep -q ":$PORT "; then
        echo -e "\e[31mError: Port $PORT is already in use on the host.\e[0m"
        echo "Please stop the service using it or choose another port."
        exit 1
    fi
done

# --- Remove container antigo se existir ---
if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "\e[33mContainer $CONTAINER_NAME already exists. Removing...\e[0m"
    podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# --- Executa novo container ---
echo -e "\e[36mStarting container $CONTAINER_NAME on network $NETWORK...\e[0m"
podman run -d \
  --name "$CONTAINER_NAME" \
  --hostname "$CONTAINER_NAME" \
  --network "$NETWORK" \
  -p ${SSH_PORT}:22 \
  -p ${APP_PORT}:5555 \
  -e SSH_PASSWORD=changeme \
  rffusion-debian12-python

# --- Exibe status final ---
echo -e "\e[32mContainer started successfully!\e[0m"
echo -e "\e[36mListing active containers:\e[0m"
podman ps

# --- Mostra mapeamento de portas e IP interno ---
echo -e "\n\e[36mNetwork details for $CONTAINER_NAME:\e[0m"
podman port "$CONTAINER_NAME"
echo -e "\nContainer IP:"
podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CONTAINER_NAME"
