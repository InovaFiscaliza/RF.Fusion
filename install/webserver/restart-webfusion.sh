#!/usr/bin/env bash
# Restart the existing container and restore legacy upload mounts afterward.
set -Eeuo pipefail

scriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
podman restart rffusion-web
bash "${scriptDir}/ensure-upload-mount.sh"
