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
