# Debian 12 + MariaDB (Podman / WSL2)

This guide explains how to build and run a **Debian 12 + MariaDB** container on **Windows** using **Podman Machine (WSL2)**.  
It includes Podman/WSL2 setup, network creation, image build, deploy/run, and database initialization.

---
## 1) Script to Install Podman Machine and Configure WSL

1. **Check Podman installation**  
   Verifies if Podman is installed. If not, installs it automatically via PowerShell (`winget`).  

2. **Check WSL2 configuration**  
   Confirms if WSL2 is set as the default version. Updates the system if necessary.  

3. **Configure vsyscall**  
   Adds `vsyscall=emulate` in `.wslconfig` for compatibility with older Debian containers. Restarts WSL and Podman machine.  

4. **Check Podman machine**  
   Validates if the Podman machine is already running. If not, initializes it with `podman machine init --now`.  

5. **Finish setup**  
   Completes the setup process and displays a success message.  

Copy the code below into a file named **`setup-podman.ps1`**:


```powershell
# setup-podman.ps1
Write-Host "=== [1/5] Checking Podman installation ==="
if (Get-Command podman -ErrorAction SilentlyContinue) {
    Write-Host "Podman is already installed."
} else {
    Write-Host "Installing Podman via winget..."
    winget install -e --id RedHat.Podman -h
}

Write-Host "=== [2/5] Checking WSL2 ==="
$wslVersion = (wsl --status 2>$null | Select-String "Default Version") -replace "[^0-9]", ""
if ($wslVersion -eq "2") {
    Write-Host "WSL2 is already set as default."
} else {
    Write-Host "Setting WSL2 as default..."
    wsl --set-default-version 2
}

Write-Host "=== [3/5] Configuring vsyscall=emulate ==="
$confFile = "$env:USERPROFILE\.wslconfig"
if ((Test-Path $confFile) -and (Select-String "vsyscall=emulate" $confFile -Quiet)) {
    Write-Host "vsyscall=emulate already configured."
} else {
    $conf = @"
[wsl2]
kernelCommandLine=vsyscall=emulate
"@
    Set-Content -Path $confFile -Value $conf -Encoding ASCII -Force
    Write-Host "vsyscall=emulate configured. Restarting WSL..."
    podman machine stop 2>$null
    wsl --shutdown
    podman machine start
}

Write-Host "=== [4/5] Checking Podman machine ==="
if ((podman machine list | Select-String "Running") -ne $null) {
    Write-Host "Podman machine is already running."
} else {
    podman machine init --now
}

Write-Host "=== [5/5] Setup completed successfully ==="
```

### ▶️ Running the Script

To execute the script, open **PowerShell** in the project directory and run:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup-podman.ps1
```

---

## 2) Setup Network

If this is your first time installing the repository containers, you need to configure a local network to enable communication between the host and the containers.  

![Network Architecture](/docs/images/HLD_RFFusion.svg)

Save the code below into a file named **`setup-network.ps1`**:

```powershell
# setup-network.ps1
# Force context to "podman-machine-default-root"
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$networkName = "rede-direct"
$expectedSubnet = "172.21.48.0/20"
$expectedGateway = "172.21.48.1"

Write-Host "=== Checking network $networkName ==="
$network = podman network inspect $networkName 2>$null

if ($LASTEXITCODE -eq 0) {
    if ($network | Select-String $expectedSubnet -Quiet) {
        Write-Host "Network $networkName already exists and is correct."
    } else {
        Write-Host "Network $networkName exists but is incorrect. Recreating..."
        podman network rm $networkName
        podman network create -d ipvlan -o parent=eth0 -o mode=l2 `
          --subnet $expectedSubnet --gateway $expectedGateway $networkName
    }
} else {
    Write-Host "Network $networkName not found. Creating..."
    podman network create -d ipvlan -o parent=eth0 -o mode=l2 `
      --subnet $expectedSubnet --gateway $expectedGateway $networkName
}

Write-Host "Network $networkName configured successfully."
````

### ▶️ Running the Script

To execute the script, open **PowerShell** in the project directory and run:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup-network.ps1
```

## 3) Files in this project

- **`Containerfile`** — Builds the Debian 12–based image (system tools, MariaDB, OpenSSH) and sets the entrypoint.
- **`docker-entrypoint.sh`** — Initializes **SSH** (root password) and **MariaDB** (root/app users, database) at container start.  
  Defaults used if not overridden: `SSH_PASSWORD=changeme`, `MARIADB_ROOT_PASSWORD=changeme`. <!-- cites entrypoint --> :contentReference[oaicite:0]{index=0}
- **`deploy-debian12-mariadb.ps1`** — Deployment/lifecycle script (see below).
- SQL files (your project): e.g., `createMeasureDB.sql`, `createProcessingDB.sql`, used to seed the database after deploy. <!-- cites deploy snippet --> :contentReference[oaicite:1]{index=1}

---

## 4) Network architecture (summary)

- **Podman Machine (WSL2)** acts as runtime and gateway.
- Custom network **`rede-direct`** (`172.21.48.0/20`, gateway `172.21.48.1`).
- Container gets a **fixed IP**, e.g., **`172.21.48.37`** (MariaDB).

---

## 5) Containerfile 

The **Containerfile** builds a Debian 12–based image, installs **MariaDB Server** and **OpenSSH**, and sets `docker-entrypoint.sh` as the entrypoint.  
The entrypoint generates SSH host keys if needed, sets the **root SSH password**, initializes MariaDB data dir on first run, creates **`appdb`**, and a sample **`appdb`** user. <!-- entrypoint reference --> :contentReference[oaicite:2]{index=2}

> You can override defaults at runtime with environment variables:  
> `MARIADB_ROOT_PASSWORD`, `SSH_PASSWORD`.

```Dockerfile
# ============================================================
# Containerfile: Debian 12 + MariaDB + SSH
# ============================================================

FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8

# --- APT: mirrors HTTPS (evita 403) ---
RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      mv /etc/apt/sources.list.d/debian.sources /etc/apt/debian.sources.bak; \
    fi; \
    printf 'deb http://deb.debian.org/debian bookworm main\n' > /etc/apt/sources.list; \
    apt-get -o Acquire::Retries=3 update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    update-ca-certificates; \
    mkdir -p /etc/apt/sources.list.d; \
    cat > /etc/apt/sources.list.d/debian.sources <<'EOF'
Types: deb
URIs: https://deb.debian.org/debian
Suites: bookworm bookworm-updates
Components: main
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg

Types: deb
URIs: https://security.debian.org/debian-security
Suites: bookworm-security
Components: main
Signed-By: /usr/share/keyrings/debian-archive-keyring.gpg
EOF

# --- Pacotes base: SSH + MariaDB + utilitários ---
RUN set -eux; \
    rm -f /etc/apt/sources.list; \
    apt-get -o Acquire::Retries=3 update; \
    apt-get install -y --no-install-recommends \
      bash curl tini \
      openssh-server openssh-client iproute2 iputils-ping netcat-openbsd \
      mariadb-server mariadb-client; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /var/run/sshd

# --- Config MariaDB ---
RUN sed -ri "s/^(bind-address\s*=).*$/\\1 0.0.0.0/" /etc/mysql/mariadb.conf.d/50-server.cnf || \
    echo -e "[mysqld]\nbind-address=0.0.0.0" > /etc/mysql/mariadb.conf.d/50-server.cnf

# --- Config SSH ---
RUN sed -ri 's/^#?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
 && sed -ri 's/^#?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -ri 's@^#?AuthorizedKeysFile .*@AuthorizedKeysFile .ssh/authorized_keys@' /etc/ssh/sshd_config

# --- Entrypoint ---
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh \
 && chmod +x /usr/local/bin/docker-entrypoint.sh

# Portas expostas
EXPOSE 22 3306

# Volumes
VOLUME ["/var/lib/mysql"]
VOLUME ["/srv/app_volume"]

WORKDIR /app

ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/sbin/sshd","-D","-e"]

```
The `docker-entrypoint.sh` script prepares the container at startup: it configures and starts SSH (generating host keys if needed and applying the root password from SSH_PASSWORD), initializes MariaDB on first run (setting MARIADB_ROOT_PASSWORD and optionally executing seed SQL files from mounted volumes), and keeps the main services running in the foreground so the container stays alive. Behavior is controlled via environment variables for easy development and deployment.
```Dockerfile
#!/usr/bin/env bash
set -Eeuo pipefail

echo "=== [entrypoint] init MariaDB + SSH container ==="

# -------------------------------------------------------------------
# 1) SSH
# -------------------------------------------------------------------
echo "[entrypoint] Configuring SSH..."
mkdir -p /var/run/sshd
chmod 755 /var/run/sshd

if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "[entrypoint] Generating SSH host keys..."
    ssh-keygen -A
fi

echo "root:${SSH_PASSWORD:-changeme}" | chpasswd

# -------------------------------------------------------------------
# 2) MariaDB
# -------------------------------------------------------------------
echo "[entrypoint] Configuring MariaDB..."
mkdir -p /var/run/mysqld
chown -R mysql:mysql /var/run/mysqld
chmod 775 /var/run/mysqld

if [ ! -d /var/lib/mysql/mysql ]; then
    echo "[entrypoint] Initializing database..."
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql > /dev/null
fi

echo "[entrypoint] Starting temporary MariaDB..."
mysqld_safe --skip-networking --datadir=/var/lib/mysql &
pid="$!"

for i in {30..0}; do
    if mariadb -uroot --protocol=socket -e "SELECT 1;" &>/dev/null; then
        break
    fi
    echo "[entrypoint] Waiting for MariaDB..."
    sleep 1
done

if [ "$i" = 0 ]; then
    echo >&2 "[entrypoint] MariaDB init process failed."
    exit 1
fi

echo "[entrypoint] Running initialization SQL..."
mariadb --protocol=socket <<-EOSQL
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MARIADB_ROOT_PASSWORD:-changeme}';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

    CREATE DATABASE IF NOT EXISTS appdb;
    CREATE USER IF NOT EXISTS 'appdb'@'%' IDENTIFIED BY 'changeme';
    GRANT ALL PRIVILEGES ON appdb.* TO 'appdb'@'%';
    FLUSH PRIVILEGES;
EOSQL

echo "[entrypoint] Shutting down temporary MariaDB..."
mysqladmin --protocol=socket -uroot -p"${MARIADB_ROOT_PASSWORD:-changeme}" shutdown

# -------------------------------------------------------------------
# 3) Subir serviços finais
# -------------------------------------------------------------------
echo "[entrypoint] Starting MariaDB..."
mysqld_safe --datadir=/var/lib/mysql --bind-address=0.0.0.0 &

echo "[entrypoint] Starting SSH..."
exec /usr/sbin/sshd -D -e

```
---

## 6) Deployment (single command)

Before running the container, **replace the host path** in the `-v` flag with the absolute path to your local repository.  Only the **left side** of the mapping should change (Windows host path); keep the **right side** (`/srv/app_volume`) as is. Example
```
 -v "C:\your_repo\GitHub\RF.Fusion\src\appCataloga\server_volume:/srv/app_volume" 
 ```

```powershell
<# =======================================================================
 Script: deploy-debian12-mariadb.ps1
 Objetivo: Build do zero e deploy do container Debian 12 + MariaDB + SSH
 ======================================================================= #>

param(
    [string]$ContainerName = "debian12-mariadb",
    [string]$ImageName     = "debian12-mariadb",
    [string]$NetworkName   = "rede-direct",
    [string]$IPAddress     = "172.21.48.37",
    [string]$SSHPassword   = "changeme",
    [string]$DBPassword    = "changeme"
)

$ErrorActionPreference = "Stop"

Write-Host "=== Building image from scratch (no cache) ===" -ForegroundColor Cyan
# Build no diretório atual (onde está o Containerfile)
podman build --no-cache -t $ImageName .

Write-Host "=== Deploying Debian 12 MariaDB container ===" -ForegroundColor Cyan

# STEP 1: Switch context
podman context use podman-machine-default-root | Out-Null

# STEP 2: Stop/remove old container
$containerExists = podman ps -a --format "{{.Names}}" | Select-String -SimpleMatch $ContainerName
if ($containerExists) {
    Write-Host "Container $ContainerName found. Stopping/removing..."
    podman stop $ContainerName -t 5 | Out-Null
    podman rm $ContainerName -f | Out-Null
}

# STEP 3: Run new container
podman run -d `
  --name $ContainerName `
  --hostname $ContainerName `
  --network $NetworkName `
  --ip $IPAddress `
  -e MARIADB_ROOT_PASSWORD=$DBPassword `
  -e SSH_PASSWORD=$SSHPassword `
  -p 2222:22 -p 3306:3306 `
  -v "C:\your_repo\GitHub\RF.Fusion\src\appCataloga\server_volume:/srv/app_volume" `
  ${ImageName}:latest

# STEP 4: Test connections
Start-Sleep -Seconds 5
Write-Host "Testing SSH (localhost:2222)..."
Test-NetConnection -ComputerName "localhost" -Port 2222 | Select-Object ComputerName, RemotePort, TcpTestSucceeded
Write-Host "Testing MariaDB (localhost:3306)..."
Test-NetConnection -ComputerName "localhost" -Port 3306 | Select-Object ComputerName, RemotePort, TcpTestSucceeded

```

### ▶️ Running the Script

To execute the script, open **PowerShell** in the project directory and run:
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy-debian12-mariadb.ps1
```

## 7) Start the Container and MariaDB database

Once the container **`debian12-mariadb`** is running, you can access it in two different ways:

#### 🔹 Using Podman Exec (direct shell access)

```powershell
# open a shell inside the container
podman exec -it debian12-mariadb bash
```

#### 🔹 Using SSH

To test SSH connectivity, execute the command below. Use the credentials for the first login as `root` and password `changeme`.

```powershell
ssh root@172.21.48.37
```

#### 🔹 Create Database

```powershell
# open a shell inside the container
podman exec -it debian12-mariadb bash

# inside the container (example paths)
mysql -uroot -pchangeme < /srv/app_volume/tmp/appCataloga/createMeasureDB.sql
mysql -uroot -pchangeme < /srv/app_volume/tmp/appCataloga/createProcessingDB.sql
```

## 8) Saving the Current State of the Container

After configuring and running the container, you may want to **save its current state** as a new image.  
This allows you to reuse the customized container later without repeating the setup.


```powershell
# Stop the container
podman stop debian12-mariadb

# Commit the container to a new image
podman commit debian12-mariadb debian12-mariadb-snapshot

# Verify
podman images

# (Optional) Restart the container
podman start debian12-mariadb
```