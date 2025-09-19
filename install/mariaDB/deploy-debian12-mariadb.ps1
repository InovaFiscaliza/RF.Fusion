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
  -v "C:\Users\augustopeterle\OneDrive - ANATEL\Documentos\GitHub\RF.Fusion\src\appCataloga\server_volume:/srv/app_volume" `
  ${ImageName}:latest

# STEP 4: Test connections
Start-Sleep -Seconds 5
Write-Host "Testing SSH (localhost:2222)..."
Test-NetConnection -ComputerName "localhost" -Port 2222 | Select-Object ComputerName, RemotePort, TcpTestSucceeded
Write-Host "Testing MariaDB (localhost:3306)..."
Test-NetConnection -ComputerName "localhost" -Port 3306 | Select-Object ComputerName, RemotePort, TcpTestSucceeded
