# Debian 12 (Bookworm) on Podman/WSL2 — Static IP, SSH, and ping (from scratch)

This guide shows how to create a **Debian 12 (Wheezy)** container **from scratch** using **Podman** (running over **WSL2**), with:
- image obtained from **Docker Hub**  
- WSL2 kernel adjusted for compatibility (**`vsyscall=emulate`**)  
- **ipvlan (mode=l2)** network on WSL's /20 subnet  
- container with **static IP**, **SSH** via **RSA key**  
- working **ping** (capabilities `NET_RAW`/`NET_ADMIN`)  

The diagram illustrates the flow from the **Host (Windows)** — PowerShell setup scripts and project files (`Containerfile`, `environment.yml`, `docker-entrypoint.sh`) — through the **Build** stage that produces the `debian12-python:latest` image, followed by **Run/Deploy** via scripts with volume mappings. The **Podman Machine (WSL2)** serves as the runtime and **gateway** for the `rede-direct` network (`172.21.48.0/20`, GW `172.21.48.1`), where the **running container** exposes services (SSH/App).

![File Structure](/docs/images/debian12-files.svg)

*Figure — Container build and runtime architecture (Debian 12/Python).*  

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
```

---
## 3) Build Container

The `build-debian12-python.ps1` file is used to build Debian12 image. This script uses other three scripts: `Containerfile`, `docker-entrypoint.sh` and `environment.yml`. Save the code below into a file named **`build-debian12-python.ps1`** to be executed in **Powershell**:

```powershell
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

```

Defines a Debian 12–based image with system utilities and OpenSSH, installs the Python environment from `environment.yml` (Conda), and sets `docker-entrypoint.sh` as the entrypoint. Exposes ports commonly used in this project (22, 5555, 8080). Copy lines below and save as **`Containerfile`**:

```Dockerfile
# Base Debian 12
FROM debian:12

ENV DEBIAN_FRONTEND=noninteractive

# ==============================
# Etapa 1: Bootstrap mínimo (HTTP)
# ==============================
RUN cat > /etc/apt/sources.list.d/debian.sources <<EOF
Types: deb
URIs: http://ftp.de.debian.org/debian
Suites: bookworm
Components: main contrib non-free
EOF

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl wget git bash bzip2 ca-certificates openssh-server && \
    rm -rf /var/lib/apt/lists/*

# ==============================
# Etapa 2: Configuração completa (HTTPS)
# ==============================
RUN cat > /etc/apt/sources.list.d/debian.sources <<EOF
Types: deb
URIs: https://deb.debian.org/debian
Suites: bookworm bookworm-updates
Components: main contrib non-free

Types: deb
URIs: https://security.debian.org/debian-security
Suites: bookworm-security
Components: main contrib non-free
EOF

RUN apt-get update

# ==============================
# Etapa 3: Instalar Miniconda
# ==============================
ENV CONDA_DIR=/opt/conda
ENV PATH=$CONDA_DIR/bin:$PATH

RUN curl -sLo ~/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh && \
    $CONDA_DIR/bin/conda clean -afy

# ==============================
# Etapa 4: Criar ambiente Conda
# ==============================
WORKDIR /app

COPY environment.yml /tmp/environment.yml
COPY docker-entrypoint.sh /usr/local/bin/entrypoint.sh

# Normaliza fins de linha (CRLF -> LF) e garante execução
RUN set -eux; \
    sed -i 's/\r$//g' /usr/local/bin/entrypoint.sh; \
    chmod +x /usr/local/bin/entrypoint.sh

# Aceitar ToS automaticamente e remover canal Intel
RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main && \
    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r && \
    sed -i '/- intel/d' /tmp/environment.yml

# Criar ambiente Conda
RUN conda env create -f /tmp/environment.yml && \
    conda clean -afy

# ==============================
# SSH: preparar diretório e validar config
# ==============================
RUN set -eux; \
    mkdir -p /var/run/sshd && chmod 755 /var/run/sshd; \
    /usr/sbin/sshd -t

# ==============================
# Ping: instalar com suid root
# ==============================
RUN apt-get update && apt-get install -y --no-install-recommends iputils-ping && \
    chmod u+s /usr/bin/ping && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ==============================
# Finalização
# ==============================
EXPOSE 22 5555 8080

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/usr/sbin/sshd", "-D"]

```
The `environment.yml` file defines Python and all dependencies to be installed inside the container. Copy the content below into **`environment.yml`**:

```yaml
name: appdata
channels:
  - conda-forge
  - intel
  - defaults
dependencies:
  - _libgcc_mutex=0.1=conda_forge
  - _openmp_mutex=4.5=2_gnu
  - binutils_impl_linux-64=2.40=hf600244_0
  - bzip2=1.0.8=h7b6447c_0
  - ca-certificates=2023.5.7=hbcca054_0
  - certifi=2023.5.7=pyhd8ed1ab_0
  - gcc=13.2.0=h574f8da_2
  - gcc_impl_linux-64=13.2.0=h338b0a0_2
  - kernel-headers_linux-64=2.6.32=he073ed8_16
  - ld_impl_linux-64=2.40=h41732ed_0
  - libffi=3.4.2=h6a678d5_6
  - libgcc-devel_linux-64=13.2.0=ha9c7c90_2
  - libgcc-ng=13.2.0=h807b86a_2
  - libgomp=13.2.0=h807b86a_2
  - libsanitizer=13.2.0=h7e041cc_2
  - libstdcxx-ng=13.2.0=h7e041cc_2
  - libuuid=1.41.5=h5eee18b_0
  - ncurses=6.3=h5eee18b_3
  - openssl=1.1.1t=h7f8727e_0
  - pip=22.2.2=py310h06a4308_0
  - python=3.10.8=h7a1cb2a_1
  - readline=8.2=h5eee18b_0
  - setuptools=65.5.0=py310h06a4308_0
  - sqlite=3.40.0=h5082296_0
  - sysroot_linux-64=2.12=he073ed8_16
  - tk=8.6.12=h1ccaba5_0
  - wheel=0.37.1=pyhd3eb1b0_0
  - xz=5.2.6=h5eee18b_0
  - zlib=1.2.13=h5eee18b_0
  - pip:
    - bcrypt==4.0.1
    - black==23.9.1
    - cffi==1.15.1
    - click==8.1.7
    - cryptography==41.0.3
    - cython==3.0.0
    - et-xmlfile==1.1.0
    - fastcore==1.5.29
    - feather-format==0.4.1
    - geographiclib==2.0
    - geopy==2.3.0
    - loguru==0.7.0
    - markdown-it-py==3.0.0
    - mdurl==0.1.2
    - mypy-extensions==1.0.0
    - mysql-connector-python==8.1.0
    - numpy==1.25.2
    - openpyxl==3.1.2
    - packaging==23.1
    - pandas==2.0.3
    - paramiko==3.3.1
    - pathspec==0.11.2
    - platformdirs==3.11.0
    - protobuf==4.21.12
    - pyarrow==12.0.1
    - pycparser==2.21
    - pygments==2.16.1
    - pynacl==1.5.0
    - python-dateutil==2.8.2
    - pytz==2023.3
    - rfpye==0.3.7
    - rich==13.5.2
    - six==1.16.0
    - tomli==2.0.1
    - typing-extensions==4.8.0
    - tzdata==2023.3
prefix: /root/miniconda3/envs/appdata
```

## 4) Run Debian12 Built Image

This script starts the **Debian 12 + Python** container built from the `Containerfile`, wiring it to the `rede-direct` network and mapping local folders as volumes for development.

- Runs the container with a fixed IP on `rede-direct` (e.g., `172.21.48.36`), restart policy, and necessary capabilities.
- Mounts host paths into the container (code/data).
- Prints connection hints when successful.

**Before running**
1. Ensure the network is set up (`setup-network.ps1`) and Podman/WSL2 configured (`setup-podman.ps1`).
2. Open `run-debian12-python.ps1` and update:
   - **`$repoRoot`** → absolute path to your local repository (e.g., `C:\your_repo\RF.Fusion`).
   

```powershell
# run-debian12-python.ps1
Write-Host "=== Switching Podman context to podman-machine-default-root ==="
podman context use podman-machine-default-root | Out-Null

$containerName = "debian12-python"
$imageName     = "debian12-python"
$containerIP   = "172.21.48.36"

# Root Git repository path (configure if needed)
$repoRoot = "C:\your_repo\GitHub\RF.Fusion"

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

```

## 5) Deploy script

This script orchestrates the **end-to-end deployment and lifecycle** of the Debian 12 + Python container by chaining the previously defined steps.

**It runs, in order:**
- `setup-podman.ps1`
- `setup-network.ps1`
- `build-debian12-python.ps1`
- `run-debian12-python.ps1`

> **Note:** `deploy-debian12-python.ps1` invokes all scripts above to ensure Podman/WSL2 is configured, the network exists, the image is built, and the container starts with the expected settings. Copy lines below and past in a **`deploy-debian12-python.ps1`**.

```powershell
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

```
### ▶️ Running the Script
To execute the script, open **PowerShell** in the project directory and run:
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy-debian12-python.ps1
```
## 6) Accessing the Created Container

Once the container **`debian12-python`** is running, you can access it in two different ways:

#### 🔹 Using Podman Exec (direct shell access)

Run the following command in **PowerShell** to open a Bash session inside the container:

```powershell
podman exec -it debian12-python bash
```
#### 🔹 Using SSH

To test SSH connectivity, execute the command below. Use the credentials for the first login as `root` and password `changeme`.

```powershell
ssh root@172.21.48.36
```

## 7) Saving the Current State of the Container

After configuring and running the container, you may want to **save its current state** as a new image.  
This allows you to reuse the customized container later without repeating the setup.


```powershell
# Stop the container
podman stop debian12-python

# Commit the container to a new image
podman commit debian12-python debian12-python-snapshot

# Verify
podman images

# (Optional) Restart the container
podman start debian12-python
```