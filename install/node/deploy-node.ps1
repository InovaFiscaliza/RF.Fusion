# ============================================================
# run-deb7-ssh-cron.ps1
# Cria e executa o container Debian 7 (SSH + Cron)
# dentro da rede rffusion-net (bridge 10.99.0.0/24)
# ============================================================

Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

# ------------------------------------------------------------
# Verifica se a rede rffusion-net existe, senão executa setup-network.ps1
# ------------------------------------------------------------
$networkName = "rffusion-net"
Write-Host "=== Checking if network '$networkName' exists ==="

$netExists = podman network inspect $networkName 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️  Network '$networkName' not found. Searching for setup-network.ps1..."

    # Detecta o diretório atual onde o script está rodando
    $currentDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

    # Procura o setup-network.ps1 no diretório atual ou subdiretórios
    $setupScript = Get-ChildItem -Path $currentDir -Recurse -Filter "setup-network.ps1" -ErrorAction SilentlyContinue | Select-Object -First 1

    if ($null -ne $setupScript) {
        Write-Host "Found setup-network.ps1 at: $($setupScript.FullName)"
        & $setupScript.FullName
        if ($LASTEXITCODE -ne 0) {
            Write-Host "❌ ERROR: setup-network.ps1 failed to create network '$networkName'. Aborting..."
            exit 1
        }
    } else {
        Write-Host "❌ ERROR: setup-network.ps1 not found in current directory or subfolders."
        Write-Host "Please make sure it exists near this script."
        exit 1
    }
} else {
    Write-Host "✅ Network '$networkName' already exists."
}

# ------------------------------------------------------------
# Configuração do container
# ------------------------------------------------------------
$containerName = "deb7-ssh-v2"
$imageName     = "debian7-ssh-cron"
$containerIP   = "10.99.0.4"
$hostSSHPort   = "2223"

# Caminho base do repositório Git no host
$repoRoot = "C:\Users\augustopeterle\OneDrive - ANATEL\Documentos\GitHub\RF.Fusion"

# Volumes mapeados
$volumes = @(
    @{ host="$repoRoot\test\mockNode\mock_volume\mnt\internal"; container="/mnt/internal" },
    @{ host="$repoRoot\src\agent\linux\AnatelUpgradePack_Node_20-6_v1"; container="/mnt/upgrade" }
)

# ------------------------------------------------------------
# Parâmetros de execução do Podman
# ------------------------------------------------------------
$arguments = @(
  "run","-d",
  "--name",$containerName,
  "--hostname",$containerName,
  "--network",$networkName,
  "--ip",$containerIP,
  "--restart=always",
  "--cap-add=NET_RAW","--cap-add=NET_ADMIN",
  "--security-opt","seccomp=unconfined",
  "--security-opt","label=disable",
  "-p", "$hostSSHPort`:22"
)

# ------------------------------------------------------------
# Verificação dos volumes
# ------------------------------------------------------------
foreach ($vol in $volumes) {
    if (!(Test-Path $vol.host)) {
        Write-Host "❌ ERROR: Directory $($vol.host) does not exist!"
        exit 1
    }
    Write-Host ("Mapped: {0} -> {1}" -f $vol.host, $vol.container)
    $arguments += @("-v", ("{0}:{1}:Z" -f $vol.host, $vol.container))
}

# ------------------------------------------------------------
# Remoção de container existente
# ------------------------------------------------------------
$exists = podman ps -a --format "{{.Names}}" | Where-Object { $_ -eq $containerName }
if ($exists) {
    Write-Host "Container $containerName found. Removing..."
    podman rm -f $containerName | Out-Null
}

# ------------------------------------------------------------
# Build da imagem customizada
# ------------------------------------------------------------
Write-Host "=== Building custom Debian 7 image with SSH and Cron ==="
podman build -f Containerfile -t $imageName .

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ ERROR: Failed to build the image. Aborting..."
    exit 1
}

# ------------------------------------------------------------
# Execução do container
# ------------------------------------------------------------
Write-Host "=== Starting container $containerName at $containerIP (SSH on host port $hostSSHPort) ==="
& podman @arguments $imageName

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Container $containerName is running at $containerIP"
    Write-Host "Access via: ssh root@localhost -p $hostSSHPort"
    Write-Host "Default password: changeme"
} else {
    Write-Host "❌ ERROR: Failed to create container $containerName"
}
