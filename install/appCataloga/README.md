# appCataloga Container

This directory contains the Linux deployment material for the `appCataloga`
runtime container.

The container is not a full self-booting application stack by itself. Its role
is to provide the operating system, Conda environment, SSH access, and mounted
project volumes required to run the `appCataloga` scripts from the shared
repository.

## Directory Layout

- [/RFFusion/install/appCataloga/linux/Containerfile](/RFFusion/install/appCataloga/linux/Containerfile)
- [/RFFusion/install/appCataloga/linux/docker-entrypoint.sh](/RFFusion/install/appCataloga/linux/docker-entrypoint.sh)
- [/RFFusion/install/appCataloga/linux/environment.yml](/RFFusion/install/appCataloga/linux/environment.yml)
- [/RFFusion/install/appCataloga/linux/deploy-debian12-python.sh](/RFFusion/install/appCataloga/linux/deploy-debian12-python.sh)

## Internal Architecture

The container provides:

1. Debian 12 base image
2. Miniconda installed under `/opt/conda`
3. Conda environment created from `environment.yml`
4. `sshd` for operational access
5. mounted RF.Fusion repository at `/RFFusion`
6. mounted repository share at `/mnt/reposfi`

The runtime is therefore:

`host volumes -> container OS + Conda -> appCataloga scripts from /RFFusion`

The actual `appCataloga` workers are started from the mounted repository,
typically through the operational scripts under:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga)

## What The Container Does Not Do

This container does not automatically start all `appCataloga` workers in the
entrypoint.

The entrypoint only:

- prepares SSH
- generates SSH host keys when needed
- applies the root password
- validates `sshd`
- hands off to the container command

So the container is a prepared runtime environment, not an orchestration layer
for the full catalog pipeline.

## Host Prerequisites

The current Linux deployment assumes:

- project repository at `/RFFusion-dev/RF.Fusion`
- repository share mounted at `/mnt/reposfi`
- `podman` installed and working
- network `podman` already available

## How To Deploy

Enter the Linux deployment directory:

```bash
cd /RFFusion/install/appCataloga/linux
```

Grant execution permission to the scripts:

```bash
chmod +x *
```

Run the deployment:

```bash
./deploy-debian12-python.sh
```

## What The Deployment Does

The deployment script:

1. switches the Podman context
2. validates the required files
3. builds the image `debian12-python`
4. removes the previous container if present
5. creates a new container with:
   - static IP `10.88.0.2`
   - SSH host port `2828`
   - application port `5555`
   - mounted repository and CIFS share
6. confirms that the container is running

## Published Services

After deployment, the expected access points are:

- SSH: `ssh root@localhost -p 2828`
- Application port: `http://localhost:5555/`

The deployment also prints the direct attach command:

```bash
podman exec -it debian12-python bash
```

## Mounted Volumes

The current deployment maps:

- `/RFFusion-dev/RF.Fusion -> /RFFusion`
- `/mnt/reposfi -> /mnt/reposfi`

Note:

- the repository volume is mounted with `:Z`
- `/mnt/reposfi` is intentionally mounted without `:Z`

## Operational Note

Once inside the container, the practical next step is usually to use the
mounted operational scripts from the repository, such as:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_stop_all.sh](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_stop_all.sh)

## Network Note

Legacy network helper scripts are no longer part of the supported deployment
flow for this container.

The current deployment script is the source of truth and expects the `podman`
network to already exist.
