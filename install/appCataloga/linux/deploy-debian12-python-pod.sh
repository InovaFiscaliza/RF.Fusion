#!/usr/bin/env bash
set -Eeuo pipefail

ContainerName="debian12-python"
ImageName="debian12-python"
PodName="rffusion-pod"

repoRoot="/RFFusion-dev/RF.Fusion"
projectRoot="$(dirname "$(realpath "$0")")"

SSHPassword="changeme"

volumes=(
  "${repoRoot}:/RFFusion:Z"
  "/mnt/reposfi:/mnt/reposfi"
)

echo "=== Ensuring POD exists ==="
if ! podman pod exists "$PodName"; then
    echo "❌ ERROR: POD $PodName does not exist. Run create-pod.sh first."
    exit 1
fi

echo "=== Building image $ImageName ==="
cd "$projectRoot"
podman build -t "$ImageName" -f Containerfile .

echo "=== Deploying container $ContainerName ==="
podman rm -f "$ContainerName" >/dev/null 2>&1 || true

args=(
    run -d
    --name "$ContainerName"
    --pod "$PodName"
    --restart=always
    -e "SSH_PASSWORD=${SSHPassword}"

    # 🔥 PUBLICAÇÃO DAS PORTAS DO PYTHON
    -p "2828:22"
    -p "5555:5555"
)

for v in "${volumes[@]}"; do
    args+=(-v "$v")
done

args+=("$ImageName")

podman "${args[@]}" >/dev/null

echo "=== ✅ Python container running inside POD ==="
echo "SSH: ssh root@localhost -p 2828"
echo "API: http://localhost:5555/"
