# RF.Fusion Web Container

This directory contains the deployment material for the `rffusion-web`
container.

The goal of this container is to publish `webfusion` from a single container
without splitting the operation across multiple external services.

## Internal Architecture

The container starts three components:

1. `nginx`
   - listens on container port `80`
   - acts as the reverse proxy for `webfusion`
   - serves large repository downloads through `X-Accel-Redirect`

2. `waitress`
   - runs the Flask application for `webfusion`
   - listens internally on `127.0.0.1:8000`
   - is not exposed directly outside the container

3. `sshd`
   - kept for operational access to the container

HTTP flow:

`client -> nginx:80 -> waitress:127.0.0.1:8000`

Download flow:

`client -> nginx -> X-Accel-Redirect -> /mnt/reposfi`

In other words, Python no longer carries the full download payload when the
file is stored in the CIFS repository mounted at `/mnt/reposfi`.

## Main Files

- [Containerfile](/RFFusion/install/webserver/Containerfile)
- [docker-entrypoint.sh](/RFFusion/install/webserver/docker-entrypoint.sh)
- [nginx-webfusion.conf](/RFFusion/install/webserver/nginx-webfusion.conf)
- [deploy-rffusionweb.sh](/RFFusion/install/webserver/deploy-rffusionweb.sh)

## Host Prerequisites

The deployment script assumes these paths already exist on the host:

- project repository at `/RFFusion-dev/RF.Fusion`
- CIFS repository mounted at `/mnt/reposfi`

It also assumes:

- `podman` is available
- the `podman` network already exists
- IP `10.88.0.34` is available
- host HTTP port `9082` is available
- host SSH port `2225` is available

## How To Deploy

Enter this directory:

```bash
cd /RFFusion/install/webserver
```

Grant execution permission to the scripts:

```bash
chmod +x *
```

Run the deployment:

```bash
./deploy-rffusionweb.sh
```

## What The Deployment Does

The script:

1. validates the expected host volumes
2. normalizes `.sh` line endings
3. builds the `rffusion-web` image
4. removes the previous container, if it exists
5. creates the new container
6. validates that the container reached `running`
7. tests:
   - `http://127.0.0.1:9082/health`
   - SSH port `2225`

## Published Service

After deployment, the expected access points are:

- Web: `http://127.0.0.1:9082`
- SSH: `ssh root@127.0.0.1 -p 2225`

On the host, the relevant mappings are:

- `/RFFusion-dev/RF.Fusion -> /RF.Fusion`
- `/mnt/reposfi -> /mnt/reposfi` (read-only)

## Operational Notes

- The container uses `--restart unless-stopped`.
- `webfusion` is considered ready only after `waitress` opens
  `127.0.0.1:8000`.
- If `waitress` or `nginx` exits, the container exits.
- If `Containerfile`, `docker-entrypoint.sh`, `nginx-webfusion.conf`, or the
  `webfusion` source code changes, the correct procedure is to recreate the
  container by running the deployment again.
