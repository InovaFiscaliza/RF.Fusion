
# Debian 7 (Wheezy) on Podman/WSL2 — Static IP, SSH, and ping (from scratch)

This guide shows how to create a **Debian 7 (Wheezy)** container **from scratch** using **Podman** (running over **WSL2**), with:
- image obtained from **Docker Hub**  
- WSL2 kernel adjusted for compatibility (**`vsyscall=emulate`**)  
- **ipvlan (mode=l2)** network on WSL's /20 subnet  
- container with **static IP**, **SSH** via **RSA key**  
- working **ping** (capabilities `NET_RAW`/`NET_ADMIN`)  
- image **committed** and **exported** to `.tar`

> **Note:** Debian 7 is old; the `vsyscall=emulate` setting is essential to avoid *segfaults* (Exit 139) with legacy glibc.

---

## 0) Install Podman on Windows and enable WSL2

In **PowerShell (Administrator)**:

```powershell
# 0.1 — Enable WSL and VM Platform
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

# 0.2 — Set WSL2 as default
wsl --set-default-version 2

# 0.3 — Install Podman (via winget)
winget install -e --id RedHat.Podman

# (recommended) Restart Windows
# shutdown /r /t 0

# 0.4 — Initialize the Podman VM (WSL2 backend)
podman machine init --now

# 0.5 — Verifications
podman --version
podman machine info
```

---

## Option 1) Create Docker image directly from Docker Hub

This option fetches a container directly from the <a href="https://hub.docker.com/r/krlmlr/debian-ssh" target="_blank">Docker Hub</a> repository. 

## 1) Pull Debian 7 image from Docker Hub

We'll use the image with **OpenSSH preinstalled**, to avoid `apt` issues on Wheezy:

```powershell
podman pull docker.io/krlmlr/debian-ssh:wheezy
```
---

## 2) Enable `vsyscall=emulate` in WSL2

Create/adjust `.wslconfig` and restart WSL:

```powershell
$conf = @"
[wsl2]
kernelCommandLine=vsyscall=emulate
"@
Set-Content -Path "$env:USERPROFILE\.wslconfig" -Value $conf -Encoding ASCII -Force

podman machine stop 2>$null
wsl --shutdown
podman machine start

podman machine ssh "cat /proc/cmdline"   # should include vsyscall=emulate
```

---

## 3) Generate **RSA** key for SSH (Wheezy doesn’t support ed25519)

```powershell
if (!(Test-Path "$env:USERPROFILE\.ssh")) { New-Item -ItemType Directory "$env:USERPROFILE\.ssh" | Out-Null }
ssh-keygen -t rsa -b 2048 -C "podman-wheezy" -f "$env:USERPROFILE\.ssh\id_rsa"
# press Enter twice to leave passphrase empty
```

---

## 4) Create **ipvlan** network (mode=L2) in WSL's /20 subnet

Discover the route to Podman VM:

```powershell
podman machine ssh "ip route"
# sample output:
# default via 172.21.48.1 dev eth0
# 172.21.48.0/20 dev eth0 proto kernel scope link src 172.21.56.34
```

Create **ipvlan** network using the observed gateway (usually `172.21.48.1`):

```powershell
podman network rm rede-direct 2>$null
podman network create -d ipvlan `
  -o parent=eth0 `
  -o mode=l2 `
  --subnet 172.21.48.0/20 `
  --gateway 172.21.48.1 `
  rede-direct

podman network inspect rede-direct
```

> Why **ipvlan**? On WSL2/Hyper-V, `macvlan` is usually blocked by vSwitch. `ipvlan` reuses eth0's MAC and works better.

---

## 5) Run the container (static IP, SSH, NET_RAW/NET_ADMIN)

Load your **public key**:

```powershell
$pub = Get-Content "$env:USERPROFILE\.ssh\id_rsa.pub" -Raw
```

Create/run the container:

```powershell
podman rm -f deb7-ssh 2>$null

podman run -d --name deb7-ssh `
  --network rede-direct `
  --ip 172.21.48.35 `
  --restart=always `
  --cap-add=NET_RAW --cap-add=NET_ADMIN `
  --entrypoint /usr/sbin/sshd `
  --security-opt seccomp=unconfined `
  --security-opt label=disable `
  docker.io/krlmlr/debian-ssh:wheezy -D
```

**Why these flags?**

- `--network rede-direct --ip 172.21.48.35`: Static IP within WSL’s /20 subnet (accessible from Windows).
- `--cap-add=NET_RAW --cap-add=NET_ADMIN`: Enables **ping** and other network tools.
- `--entrypoint /usr/sbin/sshd -D`: SSH as **PID 1** (foreground).
- `seccomp=unconfined` / `label=disable`: Reduce compatibility issues with legacy Wheezy binaries.

---

## 6) Test connectivity (Windows → container)

```powershell
arp -d *                                   # clear ARP cache (helps on first run)
ping 172.21.48.35                          # should respond <1 ms
ssh -i "$env:USERPROFILE\.ssh\id_rsa" root@172.21.48.35
# type "yes" at host key warning (first connection)
```

If you see a host key conflict (from prior `[127.0.0.1]:2222` access):
```powershell
ssh-keygen -R "[127.0.0.1]:2222"
ssh-keygen -R "172.21.48.35"
```

---

## 7) (Optional) Harden SSH

Inside the **container**:

```bash
# only key auth; no passwords
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
pkill -HUP sshd || /usr/sbin/sshd
```

If you want a **fallback password**:
```bash
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
echo 'root:changeme' | chpasswd
pkill -HUP sshd || /usr/sbin/sshd
```

---

## 8) Test ping from container (NET_RAW)

Inside the **container**:

```bash
whoami
ip addr show eth0
ping 10.1.50.51     # host Windows
```

If you see **"ping must run as root"** even as root, recreate the container with `--cap-add=NET_RAW --cap-add=NET_ADMIN` (already included above).

---

## 9) Commit and export the image

Create an image from the current container state and export it to `.tar`:

```powershell
podman commit deb7-ssh debian7:ssh-v2
podman save -o debian7-ssh-v2.tar debian7:ssh-v2
```

Reuse it on another host:

```powershell
podman load -i debian7-ssh-v2.tar
podman run -d --name deb7-ssh `
  --network rede-direct `
  --ip 172.21.48.35 `
  --restart=always `
  --cap-add=NET_RAW --cap-add=NET_ADMIN `
  --entrypoint /usr/sbin/sshd `
  --security-opt seccomp=unconfined `
  --security-opt label=disable `
  debian7:ssh-v2 -D
```

---

## Diagram (quick view)

```
Windows (X.X.X.X)
   |
   |  vEthernet (WSL) 172.21.48.1/20
   v
WSL2 / Podman VM (eth0 = 172.21.56.34/20)
   |
   |  rede-direct  (ipvlan mode=l2)  172.21.48.0/20  gw 172.21.48.1
   v
Container Debian 7  (172.21.48.35)  [sshd + NET_RAW/NET_ADMIN]
```

---

## Quick tips / Troubleshooting

- **Exit 139 / segfault (Wheezy)** → missing `vsyscall=emulate` in `.wslconfig`
- **Ping doesn’t work** → recreate container with `--cap-add=NET_RAW --cap-add=NET_ADMIN`
- **First ping fails** → run `arp -d *` in Windows and check `podman network inspect rede-direct`
- **SSH host key conflict** → use `ssh-keygen -R ...` to clear old entries

---
