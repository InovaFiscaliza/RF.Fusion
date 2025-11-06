# build-debian12-python.ps1
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

# Use script folder as project root
$projectRoot = $PSScriptRoot
$imageName   = "debian12-python"

# Check required files
$requiredFiles = @("Containerfile","docker-entrypoint.sh","environment.yml")
foreach ($file in $requiredFiles) {
    if (!(Test-Path (Join-Path $projectRoot $file))) {
        Write-Host "ERROR: Missing required file: $file in $projectRoot"
        exit 1
    }
}

Write-Host "=== Building image $imageName from Containerfile ==="
Set-Location $projectRoot
podman build -t $imageName -f (Join-Path $projectRoot "Containerfile") .

if ($LASTEXITCODE -eq 0) {
    Write-Host "Image $imageName built successfully."
} else {
    Write-Host "ERROR: Failed to build image $imageName"
    exit 1
}
