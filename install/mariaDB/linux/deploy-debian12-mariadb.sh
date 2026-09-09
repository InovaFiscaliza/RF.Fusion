#!/usr/bin/env bash
set -Eeuo pipefail

# =======================================================================
# Script: deploy-debian12-mariadb.sh
# Purpose: Build from scratch and deploy the Debian 12 + MariaDB + SSH container
# =======================================================================

# ------------------------------
# Fixed paths and container identity
# ------------------------------
CONTAINER_NAME="debian12-mariadb"
IMAGE_NAME="debian12-mariadb"
CONFIG_FILE="/etc/rffusion/rffusion.conf"
SECRET_FILE="/etc/rffusion/rffusion.secret"

# Determine script, repository and configuration directories
scriptDir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repoRoot="$(cd -- "${scriptDir}/../../.." && pwd)"
secretDir="$(dirname "${SECRET_FILE}")"
CONFIG_TEMPLATE="${repoRoot}/install/rffusion.conf"

requiredConfigKeys=(
    NETWORK_NAME
    IP_ADDRESS
    HOST_SSH_PORT
    HOST_DB_PORT
    SSH_APP_USER
)

# ------------------------------
# SQL script paths
# ------------------------------
sqlProcessing="${repoRoot}/src/mariadb/scripts/createProcessingDB.sql"
sqlMeasure="${repoRoot}/src/mariadb/scripts/createMeasureDB.sql"
sqlFusionSummary="${repoRoot}/src/mariadb/scripts/createFusionSummaryDB.sql"
sqlWebFusion="${repoRoot}/src/mariadb/scripts/createWebFusionDB.sql"

# ------------------------------
# Test and enforce host paths and secret management
# ------------------------------
if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: Run this script as root for rootful Podman."
    exit 1
fi

if [[ ! -d "${repoRoot}" ]]; then
    echo "ERROR: Repository directory not found: ${repoRoot}"
    exit 1
fi

if [[ ! -f "${scriptDir}/Containerfile" ]]; then
    echo "ERROR: Containerfile not found in ${scriptDir}"
    exit 1
fi

if [[ ! -f "${CONFIG_TEMPLATE}" ]]; then
    echo "ERROR: Configuration template not found: ${CONFIG_TEMPLATE}"
    exit 1
fi

if [[ ! -d "${secretDir}" ]]; then
    echo "[SETUP] Creating secret directory ${secretDir}..."
    mkdir -p "${secretDir}"
fi
chmod 700 "${secretDir}"

if [[ ! -e "${SECRET_FILE}" ]]; then
    echo "[SETUP] Creating secret file ${SECRET_FILE}..."
    install -m 600 /dev/null "${SECRET_FILE}"
else
    chmod 600 "${SECRET_FILE}"
fi

# Read a plain KEY=VALUE entry from a file.
get_value() {
    local file="$1"
    local key="$2"

    awk -v key="${key}" 'index($0, key "=") == 1 { sub(/^[^=]*=/, ""); print; exit }' "${file}"
}

# Replace one KEY=VALUE entry without exposing values to shell expressions.
set_value() {
    local file="$1"
    local key="$2"
    local value="$3"
    local temporaryFile

    temporaryFile="$(mktemp "${file}.tmp.XXXXXX")"
    chmod --reference="${file}" "${temporaryFile}"

    awk -v key="${key}" 'index($0, key "=") != 1 { print }' "${file}" > "${temporaryFile}"
    printf '%s=%s\n' "${key}" "${value}" >> "${temporaryFile}"

    mv -f "${temporaryFile}" "${file}"
}

# Create or complete the system configuration from the repository template.
load_configuration() {
    local key
    local value
    local missingKeys=()
    local overwriteResponse

    if [[ ! -f "${CONFIG_FILE}" ]]; then
        echo "[SETUP] Installing configuration at ${CONFIG_FILE}..."
        install -m 640 "${CONFIG_TEMPLATE}" "${CONFIG_FILE}"
    else
        for key in "${requiredConfigKeys[@]}"; do
            value="$(get_value "${CONFIG_FILE}" "${key}")"
            if [[ -z "${value}" ]]; then
                missingKeys+=("${key}")
            fi
        done

        if [[ ${#missingKeys[@]} -eq 0 ]]; then
            read -r -p "Overwrite ${CONFIG_FILE} with the repository configuration? (y/N): " overwriteResponse
            if [[ "${overwriteResponse}" =~ ^[Yy]$ ]]; then
                install -m 640 "${CONFIG_TEMPLATE}" "${CONFIG_FILE}"
            fi
        else
            for key in "${missingKeys[@]}"; do
                value="$(get_value "${CONFIG_TEMPLATE}" "${key}")"
                if [[ -z "${value}" ]]; then
                    echo "ERROR: Missing configuration value for ${key} in ${CONFIG_TEMPLATE}"
                    exit 1
                fi
                set_value "${CONFIG_FILE}" "${key}" "${value}"
            done
        fi
    fi

    for key in "${requiredConfigKeys[@]}"; do
        value="$(get_value "${CONFIG_FILE}" "${key}")"
        if [[ -z "${value}" ]]; then
            echo "ERROR: Required configuration value is empty: ${key}"
            exit 1
        fi
    done

    NETWORK_NAME="$(get_value "${CONFIG_FILE}" "NETWORK_NAME")"
    IP_ADDRESS="$(get_value "${CONFIG_FILE}" "IP_ADDRESS")"
    HOST_SSH_PORT="$(get_value "${CONFIG_FILE}" "HOST_SSH_PORT")"
    HOST_DB_PORT="$(get_value "${CONFIG_FILE}" "HOST_DB_PORT")"
    SSH_APP_USER="$(get_value "${CONFIG_FILE}" "SSH_APP_USER")"
}

load_configuration

# Load credentials from .secret or prompt the user
load_credentials() {
    echo "=== [SETUP] Loading credentials from ${SECRET_FILE} ==="

    # 1. Passwords
    SSHAppPassword=$(get_value "${SECRET_FILE}" "SSHAppPassword")
    if [[ -z "${SSHAppPassword}" ]]; then
        read -s -p "Set SSH App Password (or leave empty to use the default): " new_pass
        printf '\n'
        SSHAppPassword="${new_pass:-changeme}"
        set_value "${SECRET_FILE}" "SSHAppPassword" "${SSHAppPassword}"
    fi

    SSHPassword=$(get_value "${SECRET_FILE}" "SSHPassword")
    if [[ -z "${SSHPassword}" ]]; then
        read -s -p "Set SSH Password (or leave empty to use the default): " new_pass
        printf '\n'
        SSHPassword="${new_pass:-changeme}"
        set_value "${SECRET_FILE}" "SSHPassword" "${SSHPassword}"
    fi

    DBPassword=$(get_value "${SECRET_FILE}" "DBPassword")
    if [[ -z "${DBPassword}" ]]; then
        read -s -p "Set DB Password (or leave empty to use the default): " new_pass
        printf '\n'
        DBPassword="${new_pass:-changeme}"
        set_value "${SECRET_FILE}" "DBPassword" "${DBPassword}"
    fi

    # 2. Public keys
    SSHAppPublicKey=$(get_value "${SECRET_FILE}" "SSHAppPublicKey")
    RuntimeHealthPublicKey=$(get_value "${SECRET_FILE}" "RuntimeHealthPublicKey")

    if [[ -z "${SSHAppPublicKey}" ]]; then
        echo ""
        read -r -p "Generate a new SSH key pair for user 'rffusion' and save the public key in .secret? (y/N): " generate_key_response
        if [[ "$generate_key_response" =~ ^[Yy]$ ]]; then
            echo "[SETUP] Generating SSH keys on the host..."
            ssh-keygen -t ed25519 -f ~/.ssh/rffusion_id_rsa -N "" > /dev/null 2>&1
            NEW_PUBKEY=$(cat ~/.ssh/rffusion_id_rsa.pub)
            SSHAppPublicKey="${NEW_PUBKEY}"
            set_value "${SECRET_FILE}" "SSHAppPublicKey" "${SSHAppPublicKey}"
            echo "[SETUP] Public key saved in ${SECRET_FILE}."
        else
            echo "⚠️ Warning: The SSH App public key was not defined and will be empty. Access through this account may fail."
        fi
    fi

    if [[ -z "${RuntimeHealthPublicKey}" ]]; then
        echo ""
        read -r -p "Generate a new SSH key pair for the health check? (y/N): " generate_health_key_response
        if [[ "$generate_health_key_response" =~ ^[Yy]$ ]]; then
            echo "[SETUP] Generating SSH keys for the health check on the host..."
            ssh-keygen -t ed25519 -f ~/.ssh/rffusion_health_id_rsa -N "" > /dev/null 2>&1
            NEW_PUBKEY=$(cat ~/.ssh/rffusion_health_id_rsa.pub)
            RuntimeHealthPublicKey="${NEW_PUBKEY}"
            set_value "${SECRET_FILE}" "RuntimeHealthPublicKey" "${RuntimeHealthPublicKey}"
            echo "[SETUP] Health check public key saved in ${SECRET_FILE}."
        else
            echo "⚠️ Warning: The SSH health check public key was not defined and will be empty. The health check may fail."
        fi
    fi

}

load_credentials

# =======================================================================
# 1. Context
# =======================================================================
echo "=== [1/6] Using default Podman context ==="
podman context use default >/dev/null 2>&1 || true

podmanRootless="$(podman info --format '{{.Host.Security.Rootless}}')"
if [[ "${podmanRootless}" != "false" ]]; then
    echo "ERROR: Podman is not running in rootful mode."
    exit 1
fi

# =======================================================================
# 2. Network
# =======================================================================
echo "=== [2/6] Validating network and IP configuration ==="

# Get details of all existing networks
allNetworkDetails="$(podman network ls --format "{{.Name}}")"

# Derive subnet from IP address (assuming /24 subnet)
# Convert IP like 10.88.0.33 to subnet 10.88.0.0/24
subnet="${IP_ADDRESS%.*}.0/24"

# Validate IP address doesn't conflict with any existing network subnets
echo "[SETUP] Checking for IP conflicts across all networks..."

networkIsWellDefined=false
containerExists=false
# Check all existing networks within Podman for IP and subnet conflicts
while IFS= read -r network; do
    # exit if the network name is empty
    if [[ -n "${network}" ]]; then
        # Get network details
        networkDetail="$(podman network inspect "${network}")"

        # If the subnet is found in the network details
        if echo "${networkDetail}" | grep -Fq "\"${subnet}\""; then
        
            # if IP in use within this subnet
            if echo "${networkDetail}" | grep -Fq "\"${IP_ADDRESS}/"; then
                
                # get the name of the container using this IP
                containerUsingIP="$(podman ps -a --filter "network=${network}" --filter "ip=${IP_ADDRESS}" --format "{{.Names}}")"

                # test if the container using this IP is the target container
                if [[ "${containerUsingIP}" == "${CONTAINER_NAME}" ]]; then
                    echo "=== [2/6] IP ${IP_ADDRESS} is already in use by the existing target container ${CONTAINER_NAME} ==="
                    containerExists=true
                    break
                else
                    echo "ERROR: IP address ${IP_ADDRESS} is already in use on the target network ${NETWORK_NAME}, but not by the container ${CONTAINER_NAME}."
                    exit 1
                fi

            if [[ "${network}" == "${NETWORK_NAME}" ]]; then
                echo "=== [2/6] Subnet ${subnet} is already in use on the target network ${NETWORK_NAME} and the IP ${IP_ADDRESS} is available ==="
                networkIsWellDefined=true
                break
            else
                echo "ERROR: Subnet ${subnet} is already in use on a different network named ${network}."
                exit 1
            fi
        fi
    fi
done <<< "${allNetworkDetails}"

# test if the container exist in any network
if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    # get the current network(s) the container is connected to and ip
    containerNetworks="$(podman inspect -f '{{range .NetworkSettings.Networks}}{{.NetworkID}} {{.IPAddress}} {{end}}' "${CONTAINER_NAME}")"

    read -p "Container ${CONTAINER_NAME} already exists with the following network(s) and IP(s): ${containerNetworks}. Do you want to remove and recreate it? (y/n): " userChoice
    if [[ "${userChoice}" == "y" || "${userChoice}" == "Y" ]]; then
        echo "[SETUP] Removing existing container ${CONTAINER_NAME}..."
        podman rm -f "${CONTAINER_NAME}"
        containerExists=false
    else
        echo "ERROR: Deployment not performed as the container already exists, aborted by the user."
        exit 1
    fi
fi

# If container exists with the target IP and in the correct network, prompt the user if want to remove and recreate it or end the deployment
if [[ "${containerExists}" == true ]]; then
    read -p "Container ${CONTAINER_NAME} already exists with IP ${IP_ADDRESS}. Do you want to remove and recreate it? (y/n): " userChoice
    if [[ "${userChoice}" == "y" || "${userChoice}" == "Y" ]]; then
        echo "[SETUP] Removing existing container ${CONTAINER_NAME}..."
        podman rm -f "${CONTAINER_NAME}"
        containerExists=false
    else
        echo "Deployment unnecessary as the container already exists, aborted by the user."
        exit 1
    fi
fi

if [[ "${networkIsWellDefined}" == false ]]; then
    # Check if the target network exists but with a different subnet than the one derived from the IP address
    networkExists=false
    if [[ -n "${allNetworkDetails}" ]]; then
        if echo "${allNetworkDetails}" | grep -q "^${NETWORK_NAME}$"; then
            networkExists=true
        fi
    fi
    # If the network does not exist, create it with a subnet derived from the IP address
    if [[ "${networkExists}" == false ]]; then
        echo "[SETUP] Creating Podman network ${NETWORK_NAME}..."

        if ! podman network create --subnet "${subnet}" "${NETWORK_NAME}"; then
            echo "ERROR: Failed to create Podman network ${NETWORK_NAME} with subnet ${subnet}."
            exit 1
        fi

        echo "=== [2/6] Created Podman network ${NETWORK_NAME} with subnet ${subnet} ==="
    else
        # If the network exists, but uses a different subnet than the one derived from the IP address
        echo "[SETUP] Network ${NETWORK_NAME} already exists but uses a different subnet than the one derived from the IP address ${IP_ADDRESS}."

        #prompt the user to continue or change the network address range
        read -p "Do you want to continue with the existing network ${NETWORK_NAME}? (y/n) and change the network address range: " userChoice
        if [[ "${userChoice}" != "y" && "${userChoice}" != "Y" ]]; then
            echo "ERROR: Please check the network settings. Deployment aborted by the user."
            exit 1
        fi

        # change podman network subnet
        podman network disconnect "${NETWORK_NAME}" "${CONTAINER_NAME}" >/dev/null 2>&1 || true
        podman network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
        subnet="${IP_ADDRESS%.*}.0/24"
        if ! podman network create --subnet "${subnet}" "${NETWORK_NAME}"; then
            echo "ERROR: Failed to create Podman network ${NETWORK_NAME} with subnet ${subnet}."
            exit 1
        fi
        echo "=== [2/6] Updated Podman network ${NETWORK_NAME} with new subnet ${subnet} ==="
    fi
fi

# =======================================================================
# 3. Build the image
# =======================================================================
echo "=== [3/6] Building image ${IMAGE_NAME} ==="
if podman images --format '{{.Repository}}' | grep -q "^${IMAGE_NAME}$"; then
    echo "Removing old image..."
    podman rmi -f "${IMAGE_NAME}" >/dev/null 2>&1 || true
fi

podman build --no-cache -t "${IMAGE_NAME}" -f "${scriptDir}/Containerfile" "${scriptDir}"
if [[ $? -ne 0 ]]; then
    echo "❌ ERROR: Failed to build image ${IMAGE_NAME}"
    exit 1
fi

# =======================================================================
# 4. Deploy the container
# =======================================================================
echo "=== [4/6] Deploying container ${CONTAINER_NAME} ==="
if podman ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container ${CONTAINER_NAME} found. Removing..."
    podman rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
fi

echo "Starting new container..."
runtimeHealthArgs=()
if [[ -n "${RuntimeHealthPublicKey}" ]]; then
  runtimeHealthArgs+=(-e "RUNTIME_HEALTH_SSH_PUBLIC_KEY=${RuntimeHealthPublicKey}")
fi

podman run -d \
  --name "${CONTAINER_NAME}" \
  --hostname "${CONTAINER_NAME}" \
  --network "${NETWORK_NAME}" \
  --ip "${IP_ADDRESS}" \
  --cpus=1 \
  --memory=2g \
  --memory-swap=3g \
  --pids-limit=1024 \
  --cap-add=NET_RAW \
  --cap-add=NET_ADMIN \
  -e "MARIADB_ROOT_PASSWORD=${DBPassword}" \
  -e "SSH_PASSWORD=${SSHPassword}" \
  -e "SSH_APP_USER=${SSH_APP_USER}" \
  -e "SSH_APP_PASSWORD=${SSHAppPassword}" \
  -e "SSH_APP_PUBLIC_KEY=${SSHAppPublicKey}" \
  "${runtimeHealthArgs[@]}" \
  -p "${HOST_SSH_PORT}:2828" \
  -p "${HOST_DB_PORT}:3306" \
  -v "${repoRoot}:/RFFusion:Z" \
  "${IMAGE_NAME}:latest"

sleep 8

# =======================================================================
# 5. Verify the container
# =======================================================================
containerStatus=$(podman inspect -f '{{.State.Status}}' "${CONTAINER_NAME}")
if [[ "$containerStatus" != "running" ]]; then
    echo "❌ ERROR: Container failed to start. Current state: ${containerStatus}"
    echo "Use: podman logs ${CONTAINER_NAME}"
    exit 1
fi

echo "✅ Container is running."

# =======================================================================
# 6. Initialize the database
# =======================================================================
echo "=== [6/6] Initializing MariaDB databases ==="
podman exec -i "${CONTAINER_NAME}" bash -c "mysql -u root -p${DBPassword} < ${sqlProcessing}" || true
podman exec -i "${CONTAINER_NAME}" bash -c "mysql -u root -p${DBPassword} < ${sqlMeasure}" || true
podman exec -i "${CONTAINER_NAME}" bash -c "mysql -u root -p${DBPassword} < ${sqlFusionSummary}" || true
podman exec -i "${CONTAINER_NAME}" bash -c "mysql -u root -p${DBPassword} < ${sqlWebFusion}" || true

echo "=== ✅ Deployment completed successfully ==="
