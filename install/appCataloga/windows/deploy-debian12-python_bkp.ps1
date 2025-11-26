<# =======================================================================
 Script: deploy-debian12-python.ps1
 Objetivo: Build e inicialização do container Debian 12 com suporte a Python e SSH
 ======================================================================= #>

param(
    [string]$ContainerName = "debian12-python",
    [string]$ImageName     = "debian12-python",
    [string]$NetworkName   = "rffusion-net",
    [string]$IPAddress     = "10.99.0.2",
    [string]$SSHPassword   = "changeme",
    [string]$HostSSHPort   = "2828",
    [string]$HostAppPort   = "5555"
)

$ErrorActionPreference = "Stop"

# =======================================================================
# 1. Contexto
# =======================================================================
Write-Host "=== [1/6] Switching Podman context ==="
podman context use podman-machine-default-root | Out-Null

$repoRoot    = "C:\Users\augustopeterle\OneDrive - ANATEL\Documentos\GitHub\RF.Fusion"
$projectRoot = $PSScriptRoot
$volumes     = @(@{ host=(Join-Path $repoRoot ""); container="/RFFusion" })

# =======================================================================
# 2. Garantir rede "rffusion-net" (delegado ao setup-network.ps1)
# =======================================================================
Write-Host "=== [2/6] Checking network environment ==="
$networkScript = Join-Path $projectRoot "setup-network.ps1"

if (Test-Path $networkScript) {
    Write-Host "Running setup-network.ps1 to validate or create network..."
    & powershell -ExecutionPolicy Bypass -File $networkScript
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ ERROR: setup-network.ps1 failed to ensure network configuration."
        exit 1
    }
} else {
    Write-Host "⚠️  WARNING: setup-network.ps1 not found in $projectRoot. Skipping network setup."
}

# =======================================================================
# 3. Build da imagem
# =======================================================================
Write-Host "=== [3/6] Validating required files ==="
$requiredFiles = @("Containerfile","docker-entrypoint.sh","environment.yml")
foreach ($file in $requiredFiles) {
    if (!(Test-Path (Join-Path $projectRoot $file))) {
        Write-Host "❌ ERROR: Missing required file: $file in $projectRoot"
        exit 1
    }
}

Write-Host "=== Building image $ImageName ==="
Set-Location $projectRoot
podman build -t $ImageName -f (Join-Path $projectRoot "Containerfile") .
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ ERROR: Failed to build image $ImageName"
    exit 1
}

# =======================================================================
# 4. Deploy do container
# =======================================================================
Write-Host "=== [4/6] Deploying container $ContainerName ==="
$exists = podman ps -a --format "{{.Names}}" | Where-Object { $_ -eq $ContainerName }
if ($exists) {
    Write-Host "Container $ContainerName found. Removing..."
    podman rm -f $ContainerName | Out-Null
}

$arguments = @(
    "run","-d",
    "--name",$ContainerName,
    "--hostname",$ContainerName,
    "--network",$NetworkName,
    "--ip",$IPAddress,
    "--cap-add=NET_RAW",
    "--cap-add=NET_ADMIN",
    "--restart=always",
    "-e","SSH_PASSWORD=$SSHPassword",
    "-p","$HostSSHPort`:22",
    "-p","$HostAppPort`:5555",
)

foreach ($vol in $volumes) {
    if (!(Test-Path $vol.host)) {
        Write-Host "❌ ERROR: Directory $($vol.host) does not exist!"
        exit 1
    }
    Write-Host ("Mapped: {0} -> {1}" -f $vol.host, $vol.container)
    $arguments += @("-v", ("{0}:{1}:Z" -f $vol.host, $vol.container))
}

$arguments += @($ImageName)
Write-Host "Starting container..."
& podman @arguments | Out-Null
Start-Sleep -Seconds 5

# =======================================================================
# 5. Verificação
# =======================================================================
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Container $ContainerName is running at $IPAddress"
    Write-Host "SSH/SFTP:  ssh root@localhost -p $HostSSHPort"
    Write-Host "Python API: http://localhost:$HostAppPort/"
    Write-Host "Web UI:     http://localhost:$HostWebPort/"
    Write-Host "Attach:     podman exec -it $ContainerName bash"
} else {
    Write-Host "❌ ERROR: Failed to start container $ContainerName"
    exit 1
}

# =======================================================================
# 6. Conclusão
# =======================================================================
Write-Host "=== ✅ Deployment completed successfully ==="
