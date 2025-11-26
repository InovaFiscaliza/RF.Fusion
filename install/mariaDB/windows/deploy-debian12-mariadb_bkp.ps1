<# =======================================================================
 Script: deploy-debian12-mariadb.ps1
 Objetivo: Build do zero e deploy do container Debian 12 + MariaDB + SSH
            com inicialização automática dos bancos de dados RFDATA e RFMEASURE
 ======================================================================= #>

param(
    [string]$ContainerName = "debian12-mariadb",
    [string]$ImageName     = "debian12-mariadb",
    [string]$NetworkName   = "rffusion-net",
    [string]$IPAddress     = "10.99.0.3",
    [string]$SSHPassword   = "changeme",
    [string]$DBPassword    = "changeme",
    [string]$HostSSHPort   = "2224",     # alterado para evitar conflito
    [string]$HostDBPort    = "9081"
)

$ErrorActionPreference = "Stop"

# Caminho local para os scripts SQL (dentro do volume montado)
$sqlProcessing = "/server_volume/tmp/appCataloga/createProcessingDB-v7.sql"
$sqlMeasure    = "/server_volume/tmp/appCataloga/createMeasureDB-v3.sql"

# =======================================================================
# 1. Contexto
# =======================================================================
Write-Host "=== [1/6] Switching Podman context ===" -ForegroundColor Cyan
podman context use podman-machine-default-root | Out-Null

$projectRoot = $PSScriptRoot
$repoRoot = "/RFFusion-dev/RF.Fusion"

# =======================================================================
# 2. Garantir rede
# =======================================================================
Write-Host "=== [2/6] Ensuring network $NetworkName exists ===" -ForegroundColor Cyan
$networkScript = Join-Path $projectRoot "setup-network.ps1"
if (Test-Path $networkScript) {
    & powershell -ExecutionPolicy Bypass -File $networkScript
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ ERROR: setup-network.ps1 failed to ensure network configuration."
        exit 1
    }
} else {
    Write-Host "⚠️  WARNING: setup-network.ps1 not found. Skipping network setup."
}

# =======================================================================
# 3. Build da imagem
# =======================================================================
Write-Host "=== [3/6] Building image $ImageName ===" -ForegroundColor Cyan
$imgExists = podman images --format "{{.Repository}}" | Where-Object { $_ -eq $ImageName }
if ($imgExists) {
    Write-Host "Removing old image..."
    podman rmi -f $ImageName | Out-Null
}
podman build --no-cache -t $ImageName .
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ ERROR: Failed to build image $ImageName"
    exit 1
}

# =======================================================================
# 4. Deploy do container
# =======================================================================
Write-Host "=== [4/6] Deploying container $ContainerName ===" -ForegroundColor Cyan
$exists = podman ps -a --format "{{.Names}}" | Where-Object { $_ -eq $ContainerName }
if ($exists) {
    Write-Host "Container $ContainerName found. Removing..."
    podman rm -f $ContainerName | Out-Null
}

Write-Host "Starting new container..."
podman run -d `
  --name $ContainerName `
  --hostname $ContainerName `
  --network $NetworkName `
  --ip $IPAddress `
  --cap-add=NET_RAW `
  --cap-add=NET_ADMIN `
  -e MARIADB_ROOT_PASSWORD=$DBPassword `
  -e SSH_PASSWORD=$SSHPassword `
  -p "$HostSSHPort`:22" `
  -p "$HostDBPort`:3306" `
  -v "${repoRoot}/src/appCataloga/server_volume:/server_volume:Z" \
  ${ImageName}:latest | Out-Null

Start-Sleep -Seconds 8

# =======================================================================
# 5. Verificação do container
# =======================================================================
$containerStatus = podman inspect -f "{{.State.Status}}" $ContainerName
if ($containerStatus -ne "running") {
    Write-Host "❌ ERROR: Container failed to start. Current state: $containerStatus"
    Write-Host "Use: podman logs $ContainerName"
    exit 1
}

Write-Host "✅ Container is running."
Test-NetConnection -ComputerName "localhost" -Port $HostSSHPort | Select-Object ComputerName, RemotePort, TcpTestSucceeded
Test-NetConnection -ComputerName "localhost" -Port $HostDBPort  | Select-Object ComputerName, RemotePort, TcpTestSucceeded

# =======================================================================
# 6. Inicialização do banco de dados
# =======================================================================
Write-Host "=== [6/6] Initializing MariaDB databases ===" -ForegroundColor Cyan
podman exec -i $ContainerName bash -c "mysql -u root -p$DBPassword < $sqlProcessing"
podman exec -i $ContainerName bash -c "mysql -u root -p$DBPassword < $sqlMeasure"

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Databases successfully created and initialized."
    Write-Host "Access DB via host: 127.0.0.1:$HostDBPort (user=root, pass=$DBPassword)"
} else {
    Write-Host "⚠️  Warning: Database initialization may have failed. Check logs."
}

# =======================================================================
# Teste de conectividade interna
# =======================================================================
Write-Host "=== Testing internal network connectivity ===" -ForegroundColor Cyan
podman exec -it $ContainerName ping -c 3 10.99.0.2 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Container can reach 10.99.0.2 (Python node)."
} else {
    Write-Host "⚠️  Container cannot reach 10.99.0.2. Check bridge or capabilities."
}

Write-Host "=== ✅ Deployment completed successfully ===" -ForegroundColor Green
