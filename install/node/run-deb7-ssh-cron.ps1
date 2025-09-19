# run-deb7-ssh-cron.ps1
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$containerName = "deb7-ssh-v2"
$imageName     = "debian7-ssh-cron"
$containerIP   = "172.21.48.35"

# Caminho do repositório Git no host
$repoRoot = "C:\Users\augustopeterle\OneDrive - ANATEL\Documentos\GitHub\RF.Fusion"

# Volumes
$volumes = @(
    @{ host="$repoRoot\test\mockNode\mock_volume\mnt\internal"; container="/mnt/internal" },
    @{ host="$repoRoot\src\agent\linux\AnatelUpgradePack_Node_20-6_v1"; container="/mnt/upgrade" }
)

# Prepara args
$arguments = @(
  "run","-d",
  "--name",$containerName,
  "--network","rede-direct",
  "--ip",$containerIP,
  "--restart=always",
  "--cap-add=NET_RAW","--cap-add=NET_ADMIN",
  "--security-opt","seccomp=unconfined",
  "--security-opt","label=disable"
)

foreach ($vol in $volumes) {
    if (!(Test-Path $vol.host)) {
        Write-Host "ERROR: Directory $($vol.host) does not exist!"
        exit 1
    }
    Write-Host ("Mapped: {0} -> {1}" -f $vol.host, $vol.container)
    $arguments += @("-v", ("{0}:{1}:Z" -f $vol.host, $vol.container))
}

# Remove container antigo
$exists = podman ps -a --format "{{.Names}}" | Where-Object { $_ -eq $containerName }
if ($exists) {
    Write-Host "Container $containerName found. Removing..."
    podman rm -f $containerName | Out-Null
}

# Build da imagem customizada
Write-Host "=== Building custom Debian 7 image with SSH and Cron ==="
podman build -f Dockerfile -t $imageName .

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ ERROR: Failed to build the image. Aborting..."
    exit 1
}

# Rodar container
Write-Host "=== Starting container $containerName at $containerIP ==="
& podman @arguments $imageName

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Container $containerName is running at $containerIP"
    Write-Host "Access via: ssh root@$containerIP"
    Write-Host "Default password: changeme"
} else {
    Write-Host "❌ ERROR: Failed to create container $containerName"
}
