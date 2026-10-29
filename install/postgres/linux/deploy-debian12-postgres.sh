#!/usr/bin/env bash
set +x
set -Eeuo pipefail
source "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/common.sh"
repo=/RFFusion-dev/RF.Fusion
check=0
recreate=0
repo_seen=0
while (( $# )); do
    case "$1" in
        --repo)
            (( repo_seen == 0 )) && (( $# >= 2 )) && [[ -n "$2" && "$2" != -* ]] ||
                { postgres_error "Invalid --repo."; exit 2; }
            repo="$2"; repo_seen=1; shift 2 ;;
        --check)
            (( check == 0 && recreate == 0 )) || { postgres_error "Duplicate/conflicting option."; exit 2; }
            check=1; shift ;;
        --recreate)
            (( recreate == 0 && check == 0 )) || { postgres_error "Duplicate/conflicting option."; exit 2; }
            recreate=1; shift ;;
        *) postgres_error "Unknown deploy argument."; exit 2 ;;
    esac
done
(( recreate == 0 )) || { postgres_a1_pending; exit 1; }
postgres_load_environment "$repo"
postgres_validate_identity
[[ -z "${CONTAINER_HOST:-}${CONTAINER_CONNECTION:-}${DOCKER_HOST:-}" ]] ||
    { postgres_error "Remote socket/context is not allowed."; exit 1; }
for tool in podman python3 ip ss sha256sum df flock; do
    command -v "$tool" >/dev/null || { postgres_error "Missing tool: $tool."; exit 1; }
done
postgres_snapshot_check
[[ -r "$repo/AGENTS.md" && -d "$repo/src/mariadb/scripts" ]] ||
    { postgres_error "Repository is incomplete."; exit 1; }
repo="$(realpath "$repo")"
python3 "$POSTGRES_PACKAGE/linux/inspect.py" baseline --repo "$repo" < "$POSTGRES_PACKAGE/sql/object-map.json"
for path in "$POSTGRES_PACKAGE"/linux/*.sh "$POSTGRES_PACKAGE"/config/* "$POSTGRES_PACKAGE/linux/Containerfile"; do
    [[ -r "$path" ]] || { postgres_error "Missing package file: $path."; exit 1; }
done
podman() { command podman --remote=false "$@"; }
if podman container exists debian12-postgres; then
    postgres_error "Container exists; reuse/recreation requires resolving A1."
    exit 1
else
    status=$?
    (( status == 1 )) || { postgres_error "Cannot inspect container existence."; exit "$status"; }
fi
host_info="$(podman info --format json)"
capabilities="$(printf '%s' "$host_info" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" host)"
read -r rootless cgroups controllers <<< "$capabilities"
network_info="$(podman network inspect podman)"
printf '%s' "$network_info" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" network
container_ids="$(podman ps -a -q)"
if [[ -n "$container_ids" ]]; then
    read -r -a ids <<< "${container_ids//$'\n'/ }"
    podman inspect "${ids[@]}" | python3 "$POSTGRES_PACKAGE/linux/inspect.py" collisions
fi
ports="$(ss -H -ltn)"
if awk '{print $4}' <<< "$ports" | grep -E ':(9083|2226)$' >/dev/null; then
    postgres_error "Required TCP port is already listening."
    exit 1
fi
addresses="$(ip -o -4 addr show)"
if grep -Eq '[[:space:]]10\.88\.0\.35/' <<< "$addresses"; then
    postgres_error "Required IP is already assigned on the host."
    exit 1
fi
if command -v getenforce >/dev/null && [[ "$(getenforce)" == Enforcing ]]; then
    context="$(ls -Zd "$repo")"
    grep -Eq ':(container_file_t|container_ro_file_t|svirt_sandbox_file_t):s0([[:space:]]|$)' <<< "$context" ||
        { postgres_error "SELinux: administrator must prepare the shared read-only repository; no automatic relabel."; exit 1; }
fi
for secret in rffusion-postgres-appcataloga rffusion-postgres-webfusion rffusion-postgres-authorized-keys; do
    podman secret inspect "$secret" >/dev/null
done
data_volume="rffusion-postgres-${RFFUSION_ENV}-data"
state_volume="rffusion-postgres-${RFFUSION_ENV}-state"
for volume in "$data_volume" "$state_volume"; do
    if podman volume exists "$volume"; then
        postgres_error "Volume already exists; preserve it and resolve A1 before reuse."
        exit 1
    else
        status=$?
        (( status == 1 )) || { postgres_error "Cannot inspect volume existence."; exit "$status"; }
    fi
done
if [[ "$cgroups" == v1 ]]; then
    if [[ "$rootless" == true ]]; then controllers=""; else controllers="cpu memory pids"; fi
fi
resource_args=()
if [[ " $controllers " == *" cpu "* ]]; then resource_args+=(--cpus=1)
else printf 'WARNING: CPU limit omitted: controller unavailable.\n' >&2; fi
if [[ " $controllers " == *" memory "* ]]; then resource_args+=(--memory=2g --memory-swap=3g)
else printf 'WARNING: Memory/swap limits omitted: controller unavailable.\n' >&2; fi
if [[ " $controllers " == *" pids "* ]]; then resource_args+=(--pids-limit=1024)
else resource_args+=(--pids-limit=-1); printf 'WARNING: PIDs limit omitted: controller unavailable.\n' >&2; fi
printf 'Preflight: environment=%s IP=10.88.0.35 TCP=9083,2226 data=%s state=%s\n' \
    "$RFFUSION_ENV" "$data_volume" "$state_volume"
printf 'Requested/effective arguments: %s\n' "${resource_args[*]:-none}"
(( check == 0 )) || exit 0

podman build -t debian12-postgres -f "$POSTGRES_PACKAGE/linux/Containerfile" "$POSTGRES_PACKAGE"
for volume in "$data_volume" "$state_volume"; do
    podman volume create --label io.rffusion.component=postgres \
        --label "io.rffusion.environment=$RFFUSION_ENV" --label io.rffusion.pg-major=18 "$volume"
done
podman volume inspect "$data_volume" "$state_volume" |
    python3 "$POSTGRES_PACKAGE/linux/inspect.py" volumes --environment "$RFFUSION_ENV"
podman run -d --name debian12-postgres --hostname "$POSTGRES_HOSTNAME" \
    --label io.rffusion.component=postgres --label "io.rffusion.environment=$RFFUSION_ENV" \
    -e RFFUSION_COMPONENT=postgres -e "RFFUSION_ENV=$RFFUSION_ENV" \
    --network podman --ip 10.88.0.35 "${resource_args[@]}" \
    -p 9083:5432 -p 2226:2828 \
    -v "$repo:/RFFusion:ro" -v "$data_volume:/var/lib/postgresql/data" \
    -v "$state_volume:/srv/app_volume" \
    --secret rffusion-postgres-appcataloga,type=mount,uid=999,gid=999,mode=0400 \
    --secret rffusion-postgres-webfusion,type=mount,uid=999,gid=999,mode=0400 \
    --secret rffusion-postgres-authorized-keys,type=mount,uid=0,gid=0,mode=0400 \
    debian12-postgres
for (( attempt=0; attempt<60; attempt++ )); do
    health="$(podman inspect --format '{{.State.Status}} {{.State.Health.Status}}' debian12-postgres)"
    case "$health" in
        "running healthy")
            podman exec debian12-postgres /usr/local/bin/verify.sh
            podman inspect --format '{{json .HostConfig}}' debian12-postgres
            printf 'Fresh installation verified; restart/recreate remain gated by A1.\n'
            exit 0 ;;
        exited*|dead*|*" unhealthy")
            postgres_error "Container failed readiness; inspect logs, preserve volumes."
            exit 1 ;;
    esac
    sleep 5
done
postgres_error "Readiness timed out after 300 seconds; preserve volumes and inspect logs."
exit 1
