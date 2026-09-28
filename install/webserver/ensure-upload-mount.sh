#!/usr/bin/env bash
# Restore upload-only write access in an existing container without recreating it.
set -Eeuo pipefail

containerName="rffusion-web"
uploadFolder="/mnt/reposfi/upload"
containerPid=$(podman inspect --format '{{.State.Pid}}' "${containerName}")
if [[ ! "${containerPid}" =~ ^[0-9]+$ ]] || (( containerPid <= 1 )); then
    echo "ERROR: ${containerName} must be running."
    exit 1
fi

# Avoid requiring namespace access when the deployment already provides a writable mount.
uploadOptions=$(podman exec "${containerName}" findmnt -n -T "${uploadFolder}" -o VFS-OPTIONS)
if [[ ",${uploadOptions}," != *,rw,* ]]; then
    namespaceCommand=(nsenter)
    if [[ $(podman info --format '{{.Host.Security.Rootless}}') == true ]]; then
        namespaceCommand=(podman unshare nsenter)
    fi
    "${namespaceCommand[@]}" \
        --target "${containerPid}" --mount --pid \
        --root="/proc/${containerPid}/root" \
        --wd="/proc/${containerPid}/root" \
        /bin/sh -eu -c '
            folder=$1
            if ! mountpoint -q "$folder"; then
                mount --bind "$folder" "$folder"
            fi
            mount -o remount,bind,rw "$folder"
        ' sh "${uploadFolder}"
fi

# Verify with the container identity, not only namespace-administration privileges.
podman exec "${containerName}" /bin/sh -eu -c '
    cd /
    probe=$(mktemp "$1/.webfusion-write-check.XXXXXX")
    trap '\''rm -f -- "$probe"'\'' EXIT
    printf "WebFusion upload write check\n" > "$probe"
    findmnt -T "$1" -o TARGET,VFS-OPTIONS
' sh "${uploadFolder}"
echo "Upload storage is writable inside ${containerName}."
