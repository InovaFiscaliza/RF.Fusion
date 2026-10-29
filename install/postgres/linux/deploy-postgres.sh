#!/usr/bin/env bash
set -Eeuo pipefail

projectRoot="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repoRoot="${RFFUSION_REPO_ROOT:-$(cd "${projectRoot}/../../.." && pwd)}"
source "${projectRoot}/../../common/environment.sh"
rffusion_load_environment "$repoRoot" postgres

# Match the MariaDB bootstrap defaults; environment overrides remain available.
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-changeme}"
SSH_PASSWORD="${SSH_PASSWORD:-changeme}"
PGADMIN_DEFAULT_EMAIL="${PGADMIN_DEFAULT_EMAIL:-admin@example.com}"
PGADMIN_DEFAULT_PASSWORD="${PGADMIN_DEFAULT_PASSWORD:-changeme}"
PGADMIN_SSH_PASSWORD="${PGADMIN_SSH_PASSWORD:-changeme}"
export POSTGRES_PASSWORD SSH_PASSWORD PGADMIN_DEFAULT_EMAIL PGADMIN_DEFAULT_PASSWORD PGADMIN_SSH_PASSWORD

podName="rffusion-postgres"
containerName="rffusion-postgres"
imageName="rffusion-postgres:17-alpine"
volumeName="rffusion-postgres-data"
sshVolumeName="rffusion-postgres-ssh"
pgadminContainer="rffusion-pgadmin"
pgadminImage="rffusion-pgadmin:9.18"
pgadminVolume="rffusion-pgadmin-data"
pgadminSshVolume="rffusion-pgadmin-ssh"

# Refuse replacement to protect an existing instance and its configuration.
if podman pod exists "$podName" || podman container exists "$containerName" || podman container exists "$pgadminContainer"; then
    printf 'ERROR: Pod or a project container already exists; inspect it before replacement.\n' >&2
    exit 1
fi
podmanInfo="$(podman info --format '{{.Host.Security.Rootless}} {{.Host.CgroupsVersion}} {{range .Host.CgroupControllers}}{{.}} {{end}}')"
read -r rootless cgroupVersion controllers <<< "$podmanInfo"
if [[ "$cgroupVersion" == v1 ]]; then
    if [[ "$rootless" == true ]]; then controllers=""; else controllers="cpu memory pids"; fi
fi
resourceArgs=()
if [[ " ${controllers} " == *" cpu "* ]]; then resourceArgs+=(--cpus=1); else echo "WARNING: CPU limit unavailable."; fi
if [[ " ${controllers} " == *" memory "* ]]; then resourceArgs+=(--memory=2g --memory-swap=3g); else echo "WARNING: Memory limit unavailable."; fi
if [[ " ${controllers} " == *" pids "* ]]; then resourceArgs+=(--pids-limit=1024); else resourceArgs+=(--pids-limit=-1); echo "WARNING: PID limit unavailable."; fi
podman network inspect podman >/dev/null
podman build -t "$imageName" "${projectRoot}/postgres"
podman build -t "$pgadminImage" "${projectRoot}/pgadmin"
podman volume exists "$volumeName" || podman volume create "$volumeName" >/dev/null
podman volume exists "$sshVolumeName" || podman volume create "$sshVolumeName" >/dev/null
podman volume exists "$pgadminVolume" || podman volume create "$pgadminVolume" >/dev/null
podman volume exists "$pgadminSshVolume" || podman volume create "$pgadminSshVolume" >/dev/null
podman pod create --name "$podName" \
    --hostname "${rffusion_identity_args[1]}" \
    --network podman --ip 10.88.0.35 \
    -p 2226:22 -p 2227:2222 -p 9083:5432 -p 9084:8080 >/dev/null
podman run -d \
    --name "$containerName" \
    --pod "$podName" \
    "${rffusion_identity_args[@]:2}" \
    "${resourceArgs[@]}" \
    -e POSTGRES_PASSWORD -e SSH_PASSWORD \
    -v "${repoRoot}:/RFFusion:z" \
    -v "${volumeName}:/var/lib/postgresql/data" \
    -v "${sshVolumeName}:/var/lib/rffusion-ssh" \
    "$imageName"

# Wait for the final TCP listener, not the temporary initialization server.
ready=false
for ((attempt=1; attempt<=60; attempt++)); do
    if podman exec "$containerName" pg_isready -h 127.0.0.1 -U postgres -d postgres >/dev/null 2>&1; then
        ready=true
        break
    fi
    if [[ "$(podman inspect -f '{{.State.Status}}' "$containerName")" != running ]]; then
        echo "ERROR: PostgreSQL container stopped; inspect its logs." >&2
        exit 1
    fi
    sleep 2
done
if [[ "$ready" != true ]]; then
    echo "ERROR: PostgreSQL did not become ready; inspect its logs." >&2
    exit 1
fi

existingSchemas="$(podman exec -u postgres "$containerName" psql -X -U postgres -d postgres -Atq -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_namespace WHERE nspname IN ('bpdata','rfdata','rffusion_summary','webfusion')")"
if [[ "$existingSchemas" != 0 ]]; then
    echo "WARNING: Existing project schemas detected; bootstrap was skipped. Review them before use."
else
    if ! podman exec -u postgres "$containerName" psql -X -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction \
        -f /RFFusion/src/postgres/scripts/createProcessingDB.sql \
        -f /RFFusion/src/postgres/scripts/createMeasureDB.sql \
        -f /RFFusion/src/postgres/scripts/createFusionSummaryDB.sql \
        -f /RFFusion/src/postgres/scripts/createWebFusionDB.sql; then
        echo "ERROR: Bootstrap failed and was rolled back; container and volumes were retained." >&2
        exit 1
    fi
    echo "PostgreSQL schemas and seed data initialized."
fi
podman run -d \
    --name "$pgadminContainer" \
    --pod "$podName" \
    --label "${rffusion_identity_args[3]}" \
    --label "io.rffusion.component=pgadmin" \
    -e PGADMIN_DEFAULT_EMAIL -e PGADMIN_DEFAULT_PASSWORD -e PGADMIN_SSH_PASSWORD \
    -e PGADMIN_LISTEN_PORT=8080 \
    -v "${pgadminVolume}:/var/lib/pgadmin:U" \
    -v "${pgadminSshVolume}:/var/lib/rffusion-pgadmin-ssh" \
    "$pgadminImage"
printf 'Pod created. PostgreSQL is ready; check pgAdmin logs before connecting.\n'
