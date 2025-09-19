
# Debian 7 (Wheezy) on Podman/WSL2 — Static IP, SSH, and ping (from scratch)

This guide shows how to create a **Debian 7 (Wheezy)** container **from scratch** using **Podman** (running over **WSL2**), with:
- image obtained from **Docker Hub**  
- WSL2 kernel adjusted for compatibility (**`vsyscall=emulate`**)  
- **ipvlan (mode=l2)** network on WSL's /20 subnet  
- container with **static IP**, **SSH** via **RSA key**  
- working **ping** (capabilities `NET_RAW`/`NET_ADMIN`)  

> **Note:** Debian 7 is old; the `vsyscall=emulate` setting is essential to avoid *segfaults* (Exit 139) with legacy glibc.

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

### 3) Create Debian 7 Wheezy Docker Container

The following **Dockerfile** builds a Debian 7 (Wheezy) image and installs the required packages:  
- `cron`  
- `net-tools`  
- `openssh-server`  
- utility packages (`procps`)  

It also configures the Debian archive repositories, initializes SSH, and ensures both **cron** and **sshd** run at container startup. Copy the code below and save as **`Dockerfile`**.

```dockerfile
FROM debian:wheezy

# Configure archive repositories
RUN echo "deb http://archive.debian.org/debian wheezy main contrib non-free" > /etc/apt/sources.list \
 && echo "Acquire::Check-Valid-Until false;" > /etc/apt/apt.conf

# Install packages ignoring expired signatures
RUN apt-get update || true \
 && apt-get -o Acquire::AllowInsecureRepositories=true \
           -o Acquire::AllowDowngradeToInsecureRepositories=true \
           install -y --allow-unauthenticated \
           openssh-server cron procps net-tools \
 && mkdir -p /var/run/sshd \
 && ssh-keygen -A \
 && echo "root:changeme" | chpasswd

# Expose default SSH port
EXPOSE 22

# Start cron and sshd together
CMD service cron start && /usr/sbin/sshd -D
```
In this step, pay attention to the **`$repoRoot`** parameter. You must set it to the path of your **local repository**. For example:

```powershell
$repoRoot = "C:\your_repo\GitHub\RF.Fusion"
````
to be mapped to containers volumes `/mnt/internal` and `/mnt/upgrade`. After this step, copy the code below, replace `$repoRoot$` and save as **`run-deb7-ssh-cron.ps1`**.

``` powershell
# run-deb7-ssh-cron.ps1
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$containerName = "deb7-ssh-v2"
$imageName     = "debian7-ssh-cron"
$containerIP   = "172.21.48.35"

# Caminho do repositório Git no host
$repoRoot = "C:\your_repo\Documentos\GitHub\RF.Fusion"

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

```

### ▶️ Running the Script

To execute the script, open **PowerShell** in the project directory and run:
```powershell
powershell -ExecutionPolicy Bypass -File .\run-deb7-ssh-cron.ps1
```

### 4) Accessing the Created Container

Once the container **`deb7-ssh-v2`** is running, you can access it in two different ways:

#### 🔹 Using Podman Exec (direct shell access)

Run the following command in **PowerShell** to open a Bash session inside the container:

```powershell
podman exec -it deb7-ssh-v2 bash
```
#### 🔹 Using SSH

To test SSH connectivity, execute the command below. Use the credentials for the first login as `root` and password `changeme`.

```powershell
ssh root@172.21.48.35
```

### 5) Saving the Current State of the Container

After configuring and running the container, you may want to **save its current state** as a new image.  
This allows you to reuse the customized container later without repeating the setup.


```powershell
# Stop the container
podman stop deb7-ssh-v2

# Commit the container to a new image
podman commit deb7-ssh-v2 debian7-ssh-cron-snapshot

# Verify
podman images

# (Optional) Restart the container
podman start deb7-ssh-v2
```
