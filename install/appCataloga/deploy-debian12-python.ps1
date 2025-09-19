# deploy-debian12-python.ps1
Write-Host "=== Deploying Debian 12 Python container ==="

# Step 1: Setup Podman
Write-Host "`n--- STEP 1: Podman setup ---"
& "$PSScriptRoot\setup-podman.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to setup podman"
    exit 1
}

# Step 2: Setup network
Write-Host "`n--- STEP 2: Network setup ---"
& "$PSScriptRoot\setup-network.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to setup network"
    exit 1
}

# Step 3: Build image
Write-Host "`n--- STEP 3: Build image ---"
& "$PSScriptRoot\build-debian12-python.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to build image"
    exit 1
}

# Step 4: Run container
Write-Host "`n--- STEP 4: Run container ---"
& "$PSScriptRoot\run-debian12-python.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to run container"
    exit 1
}

# --- SSH: atualizar fingerprint do host do container (apenas este ajuste) ---
$knownHosts = Join-Path $env:USERPROFILE ".ssh\known_hosts"
$knownDir = Split-Path $knownHosts
if (-not (Test-Path $knownDir)) { New-Item -ItemType Directory -Path $knownDir -Force | Out-Null }

ssh-keygen -R $ContainerIp 2>$null | Out-Null
$scan = ssh-keyscan -H -t ed25519 $ContainerIp 2>$null
$scan | Out-File -FilePath $knownHosts -Append -Encoding ascii


Write-Host "`n=== Deployment finished successfully ==="
