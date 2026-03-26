#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/opt/conda/envs/appdata/bin/python}"

cd "$ROOT_DIR"

if [ "$#" -eq 0 ]; then
    set -- tests -q
fi

exec "$PYTHON_BIN" -m pytest "$@"
