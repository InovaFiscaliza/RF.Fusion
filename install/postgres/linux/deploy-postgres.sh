#!/usr/bin/env bash
set -Eeuo pipefail

recreate=false
resetSchemas=false
while (($#)); do
    case "$1" in
        --recreate) recreate=true ;;
        --reset-schemas) resetSchemas=true ;;
        --help|-h)
            printf 'Usage: %s [--recreate] [--reset-schemas]\n' "$0"
            printf '  --recreate      Replace project containers, preserving named volumes.\n'
            printf '  --reset-schemas Back up and rebuild the four project schemas; restore Summary usage data.\n'
            exit 0
            ;;
        *) printf 'ERROR: Unknown option: %s\n' "$1" >&2; exit 2 ;;
    esac
    shift
done

projectRoot="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repoRoot="${RFFUSION_REPO_ROOT:-$(cd "${projectRoot}/../../.." && pwd)}"
source "${projectRoot}/../../common/environment.sh"
rffusion_load_environment "$repoRoot" postgres

# Match the MariaDB bootstrap defaults; environment overrides remain available.
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-changeme}"
POSTGRES_DB="${POSTGRES_DB:-RFFUSION}"
SSH_PASSWORD="${SSH_PASSWORD:-changeme}"
PGADMIN_DEFAULT_EMAIL="${PGADMIN_DEFAULT_EMAIL:-admin@anatel.gov.br}"
PGADMIN_DEFAULT_PASSWORD="${PGADMIN_DEFAULT_PASSWORD:-changeme}"
PGADMIN_SSH_PASSWORD="${PGADMIN_SSH_PASSWORD:-changeme}"
export POSTGRES_DB POSTGRES_PASSWORD SSH_PASSWORD PGADMIN_DEFAULT_EMAIL PGADMIN_DEFAULT_PASSWORD PGADMIN_SSH_PASSWORD

podName="rffusion-postgres-pod"
legacyPodName="rffusion-postgres"
infraName="rffusion-postgres-infra"
infraImage="registry.k8s.io/pause:3.10"
containerName="rffusion-postgres"
imageName="rffusion-postgres:17-postgis-alpine"
# Use Podman's persistent storage unless a bind mount is explicitly selected.
dataDir="${RFFUSION_POSTGRES_DATA_DIR:-}"
if [[ -z "$dataDir" && -f "${repoRoot}/.env/postgres-data-dir" ]]; then
    dataDir="$(cat "${repoRoot}/.env/postgres-data-dir")"
    if [[ -z "$dataDir" || "$dataDir" == *$'\n'* || "$dataDir" == *$'\r'* ]]; then
        echo "ERROR: .env/postgres-data-dir must contain one absolute host path." >&2
        exit 1
    fi
fi
legacyVolumeName="rffusion-postgres-data"
if [[ -n "$dataDir" ]]; then
    if [[ "$dataDir" != /* ]]; then
        echo "ERROR: RFFUSION_POSTGRES_DATA_DIR must be an absolute host path." >&2
        exit 1
    fi
    dataMount="${dataDir}:/var/lib/postgresql/data:z"
else
    if [[ -f "${repoRoot}/.env/postgres-data/PG_VERSION" ]]; then
        echo "ERROR: Repository cluster found. Set RFFUSION_POSTGRES_DATA_DIR to its host path before deploying." >&2
        exit 1
    fi
    dataMount="${legacyVolumeName}:/var/lib/postgresql/data"
fi
sshVolumeName="rffusion-postgres-ssh"
pgadminContainer="rffusion-pgadmin"
pgadminImage="rffusion-pgadmin:9.18"
pgadminVolume="rffusion-pgadmin-data"
pgadminSshVolume="rffusion-pgadmin-ssh"

backupDir=""
usageTables=0
backup_database() {
    # Backups stay outside Git and survive container/volume replacement.
    local tableCount
    umask 077
    mkdir -p "${repoRoot}/.env/postgres-backups"
    backupDir="$(mktemp -d "${repoRoot}/.env/postgres-backups/recreate_$(date -u +%Y%m%dT%H%M%SZ)_XXXXXX")"
    podman exec -u postgres "$containerName" pg_dump -U postgres -d "$POSTGRES_DB" -Fc > "$backupDir/postgres-before.dump"
    podman exec -i -u postgres "$containerName" pg_restore --list < "$backupDir/postgres-before.dump" > "$backupDir/archive-list.txt"
    tableCount="$(podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -Atq -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'RFFUSION_SUMMARY' AND table_name IN ('WEBFUSION_USAGE_LOG_CHECKPOINT','WEBFUSION_USAGE_METRIC_MONTHLY') AND table_type = 'BASE TABLE'")"
    usageTables="$tableCount"
    case "$tableCount" in
        2)
            podman exec -u postgres "$containerName" pg_dump -U postgres -d "$POSTGRES_DB" \
                --data-only --column-inserts \
                -t '"RFFUSION_SUMMARY"."WEBFUSION_USAGE_LOG_CHECKPOINT"' \
                -t '"RFFUSION_SUMMARY"."WEBFUSION_USAGE_METRIC_MONTHLY"' > "$backupDir/summary-usage.sql"
            ;;
        0) : > "$backupDir/summary-usage.sql" ;;
        *) echo "ERROR: Both Summary usage tables must exist together; nothing was replaced." >&2; return 1 ;;
    esac
    (cd "$backupDir" && sha256sum postgres-before.dump summary-usage.sql > SHA256SUMS)
    printf 'PostgreSQL backup: %s\n' "$backupDir"
}

existing=false
backupAfterStart=false
if podman pod exists "$podName" || podman pod exists "$legacyPodName" || podman container exists "$containerName" || podman container exists "$pgadminContainer"; then
    existing=true
    if [[ "$recreate" != true ]]; then
        echo "ERROR: Project containers already exist. Use --recreate to replace them, preserving volumes." >&2
        exit 1
    fi
    # A failed backup must leave a running database untouched. A partial pod
    # has no live database to dump; retain its volumes and back up after startup.
    if podman container exists "$containerName" && \
        [[ "$(podman inspect -f '{{.State.Running}}' "$containerName")" == true ]]; then
        backup_database
    else
        backupAfterStart=true
        echo "Partial or stopped installation: volumes retained; database backup will run after startup."
    fi
fi

podmanInfo="$(podman info --format '{{.Host.Security.Rootless}} {{.Host.CgroupsVersion}} {{range .Host.CgroupControllers}}{{.}} {{end}}')"
read -r rootless cgroupVersion controllers <<< "$podmanInfo"
if [[ "$cgroupVersion" == v1 ]]; then
    if [[ "$rootless" == true ]]; then controllers=""; else controllers="cpu memory pids"; fi
fi
resourceArgs=()
if [[ " ${controllers} " == *" cpu "* ]]; then resourceArgs+=(--cpus=1); else echo "WARNING: CPU limit unavailable."; fi
if [[ " ${controllers} " == *" memory "* ]]; then resourceArgs+=(--memory=2g --memory-swap=3g); else echo "WARNING: Memory limit unavailable."; fi
if [[ " ${controllers} " == *" pids "* ]]; then resourceArgs+=(--pids-limit=1024); else echo "WARNING: PID limit unavailable."; fi
podman network inspect podman >/dev/null
podman build --pull=always -t "$imageName" "${projectRoot}/postgres"
podman build --pull=always -t "$pgadminImage" "${projectRoot}/pgadmin"
# Fetch the infra image before replacing an existing pod.
podman pull "$infraImage"
# Build successfully before stopping an existing installation.
if [[ "$existing" == true ]]; then
    for existingPod in "$podName" "$legacyPodName"; do
        if podman pod exists "$existingPod"; then podman pod rm -f "$existingPod"; fi
    done
    for existingContainer in "$containerName" "$pgadminContainer"; do
        if podman container exists "$existingContainer"; then podman rm -f "$existingContainer"; fi
    done
fi
podman volume exists "$sshVolumeName" || podman volume create "$sshVolumeName" >/dev/null
podman volume exists "$pgadminVolume" || podman volume create "$pgadminVolume" >/dev/null
podman volume exists "$pgadminSshVolume" || podman volume create "$pgadminSshVolume" >/dev/null
umask 077
if [[ -z "$dataDir" ]]; then
    podman volume exists "$legacyVolumeName" || podman volume create "$legacyVolumeName" >/dev/null
else
    mkdir -p "$dataDir"
fi
if [[ -n "$dataDir" ]] && podman volume exists "$legacyVolumeName"; then
    # Runs inside the image so ownership and permissions survive in rootful and
    # rootless Podman. A non-empty directory is never overwritten, and a failed
    # copy is emptied so a partial cluster is never started.
    seedResult="$(podman run --rm --user 0 --entrypoint sh \
        -v "${legacyVolumeName}:/source:ro" -v "${dataDir}:/target:z" "$imageName" -c '
        if [ -n "$(ls -A /target)" ]; then echo kept; exit 0; fi
        if cp -a /source/. /target/ && test -f /target/PG_VERSION &&
            test "$(stat -c "%u:%g:%a" /source)" = "$(stat -c "%u:%g:%a" /target)"; then echo copied
        else find /target -mindepth 1 -delete; echo failed; fi')" || seedResult="failed"
    case "$seedResult" in
        copied) printf 'PostgreSQL data copied from volume %s to %s; the volume was kept.\n' "$legacyVolumeName" "$dataDir" ;;
        kept) printf 'Using existing PostgreSQL data in %s.\n' "$dataDir" ;;
        *) echo "ERROR: PostgreSQL data copy failed; the volume ${legacyVolumeName} is unchanged." >&2; exit 1 ;;
    esac
fi
podman pod create --name "$podName" \
    --infra-name "$infraName" \
    --infra-image "$infraImage" \
    --infra-command /pause \
    --hostname "${rffusion_identity_args[1]}" \
    --network podman --ip 10.88.0.35 \
    -p 2226:22 -p 2227:2222 -p 9083:5432 -p 9084:8080 >/dev/null
podman run -d \
    --name "$containerName" \
    --pod "$podName" \
    "${rffusion_identity_args[@]:2}" \
    "${resourceArgs[@]}" \
    -e POSTGRES_DB -e POSTGRES_PASSWORD -e SSH_PASSWORD \
    -v "${repoRoot}:/RFFusion:z" \
    -v "$dataMount" \
    -v "${sshVolumeName}:/var/lib/rffusion-ssh" \
    "$imageName"

# Wait for the final TCP listener, not the temporary initialization server.
ready=false
for ((attempt=1; attempt<=60; attempt++)); do
    if podman exec "$containerName" pg_isready -h 127.0.0.1 -U postgres -d "$POSTGRES_DB" >/dev/null 2>&1; then
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

if [[ "$backupAfterStart" == true ]]; then backup_database; fi

postgisAvailable="$(podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -Atq -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_available_extensions WHERE name = 'postgis'")"
if [[ "$postgisAvailable" != 1 ]]; then
    echo "ERROR: PostGIS is missing from the new image; no schemas were reset." >&2
    exit 1
fi

existingSchemas="$(podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -Atq -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_namespace WHERE nspname IN ('bpdata','rfdata','rffusion_summary','webfusion','BPDATA','RFDATA','RFFUSION_SUMMARY','WEBFUSION')")"
if [[ "$resetSchemas" == true ]]; then
    legacySchemas="$(podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -Atq -v ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_namespace WHERE nspname IN ('bpdata','rfdata','rffusion_summary','webfusion')")"
    if [[ "$legacySchemas" != 0 ]]; then
        echo "ERROR: Lowercase schemas require an explicit data migration; nothing was reset." >&2
        exit 1
    fi
    backup_database
    resetSql="$backupDir/recreate.sql"
    printf "SET lock_timeout = '5s';\n" > "$resetSql"
    if [[ "$usageTables" == 2 ]]; then
        cat >> "$resetSql" <<'SQL'
LOCK TABLE "RFFUSION_SUMMARY"."WEBFUSION_USAGE_LOG_CHECKPOINT", "RFFUSION_SUMMARY"."WEBFUSION_USAGE_METRIC_MONTHLY" IN ACCESS EXCLUSIVE MODE;
CREATE TEMP TABLE rffusion_usage_checkpoint ON COMMIT DROP AS TABLE "RFFUSION_SUMMARY"."WEBFUSION_USAGE_LOG_CHECKPOINT";
CREATE TEMP TABLE rffusion_usage_monthly ON COMMIT DROP AS TABLE "RFFUSION_SUMMARY"."WEBFUSION_USAGE_METRIC_MONTHLY";
SQL
    fi
    cat >> "$resetSql" <<'SQL'
DROP SCHEMA IF EXISTS "WEBFUSION" CASCADE;
DROP SCHEMA IF EXISTS "RFFUSION_SUMMARY" CASCADE;
DROP SCHEMA IF EXISTS "RFDATA" CASCADE;
DROP SCHEMA IF EXISTS "BPDATA" CASCADE;
\i /RFFusion/src/postgres/scripts/createProcessingDB.sql
\i /RFFusion/src/postgres/scripts/createMeasureDB.sql
\i /RFFusion/src/postgres/scripts/createFusionSummaryDB.sql
\i /RFFusion/src/postgres/scripts/createWebFusionDB.sql
SQL
    if [[ "$usageTables" == 2 ]]; then
        cat >> "$resetSql" <<'SQL'
INSERT INTO "RFFUSION_SUMMARY"."WEBFUSION_USAGE_LOG_CHECKPOINT" ("NA_SOURCE_NAME", "NA_LOG_PATH", "NA_FILE_SIGNATURE", "NU_LAST_OFFSET", "NU_LAST_SIZE", "NU_LAST_MTIME_NS", "DT_UPDATED_AT")
SELECT "NA_SOURCE_NAME", "NA_LOG_PATH", "NA_FILE_SIGNATURE", "NU_LAST_OFFSET", "NU_LAST_SIZE", "NU_LAST_MTIME_NS", "DT_UPDATED_AT" FROM pg_temp.rffusion_usage_checkpoint;
INSERT INTO "RFFUSION_SUMMARY"."WEBFUSION_USAGE_METRIC_MONTHLY" ("NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE", "DT_UPDATED_AT")
SELECT "NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE", "DT_UPDATED_AT" FROM pg_temp.rffusion_usage_monthly;
SQL
    fi
    if ! podman exec -i -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" \
        -v ON_ERROR_STOP=1 --single-transaction -f - < "$resetSql"; then
        echo "ERROR: Schema recreation failed and was rolled back; backup and containers were retained." >&2
        exit 1
    fi
    echo "Project schemas recreated; Summary usage counters and checkpoints restored."
elif [[ "$existingSchemas" != 0 ]]; then
    echo "Existing project schemas preserved. Use --reset-schemas to rebuild them after backup."
else
    if ! podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 --single-transaction \
        -f /RFFusion/src/postgres/scripts/createProcessingDB.sql \
        -f /RFFusion/src/postgres/scripts/createMeasureDB.sql \
        -f /RFFusion/src/postgres/scripts/createFusionSummaryDB.sql \
        -f /RFFusion/src/postgres/scripts/createWebFusionDB.sql; then
        echo "ERROR: Bootstrap failed and was rolled back; container and volumes were retained." >&2
        exit 1
    fi
    echo "PostgreSQL schemas and seed data initialized."
fi
podman exec -u postgres "$containerName" psql -X -U postgres -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
    -c 'CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;' \
    -c 'SELECT public.postgis_full_version();'
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
