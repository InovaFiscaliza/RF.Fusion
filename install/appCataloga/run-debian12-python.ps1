# run-debian12-python.ps1
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$containerName = "debian12-python"
$imageName     = "debian12-python"
$containerIP   = "172.21.48.36"

# Root Git repository path (configure if needed)
$repoRoot = "C:\Users\augustopeterle\OneDrive - ANATEL\Documentos\GitHub\RF.Fusion"

# Volume relative to repo → container destination
$volumes = @(
    @{ host=(Join-Path $repoRoot ""); container="/RFFusion" }
)

# ================= Prepare volumes =================
Write-Host "=== Preparing Git repository volumes ==="
$arguments = @(
  "run","-d",
  "--name",$containerName,
  "--network","rede-direct",
  "--ip",$containerIP,
  "--cap-add=NET_RAW",
  "--cap-add=NET_ADMIN"
  "--restart=always"
)

foreach ($vol in $volumes) {
    if (!(Test-Path $vol.host)) {
        Write-Host "ERROR: Directory $($vol.host) does not exist!"
        exit 1
    }
    Write-Host ("Mapped: {0} -> {1}" -f $vol.host, $vol.container)
    $arguments += @("-v", ("{0}:{1}:Z" -f $vol.host, $vol.container))
}

# ================= Remove existing container =================
Write-Host "=== Checking if container $containerName exists ==="
$exists = podman ps -a --format "{{.Names}}" | Where-Object { $_ -eq $containerName }
if ($exists) {
    Write-Host "Container $containerName found. Removing..."
    podman rm -f $containerName | Out-Null
}

# ================= Run container =================
Write-Host "=== Running container $containerName ==="
$arguments += @($imageName)
& podman @arguments

if ($LASTEXITCODE -eq 0) {
    Write-Host "=== Container $containerName is running at $containerIP ==="
    Write-Host "Access with: ssh root@$containerIP -p 2222"
    Write-Host "Or: podman exec -it $containerName bash"
} else {
    Write-Host "ERROR: Failed to run container $containerName"
}
