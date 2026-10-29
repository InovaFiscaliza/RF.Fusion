"""Strict, service-free interpretation of Podman JSON for the installation scripts."""

import argparse
import hashlib
import ipaddress
import json
import shutil
import sys
from pathlib import Path


def target(data: list[dict], expected_name: str) -> None:
    """Validate the explicitly named disposable development container."""
    if len(data) != 1:
        raise ValueError("Exactly one container is required")
    entry = data[0]
    if entry["Name"].lstrip("/") != expected_name:
        raise ValueError("Container name is not an exact match")
    labels = entry["Config"]["Labels"] or {}
    if labels.get("io.rffusion.component") != "postgres" or labels.get("io.rffusion.environment") != "development":
        raise ValueError("Only a development postgres container is disposable")
    variables = set(entry["Config"]["Env"])
    if not {"RFFUSION_ENV=development", "RFFUSION_COMPONENT=postgres"} <= variables:
        raise ValueError("Container environment disagrees with identity labels")
    if not entry["State"]["Running"]:
        raise ValueError("Container is not running")
    expected = {
        "/var/lib/postgresql/data": "rffusion-postgres-development-data",
        "/srv/app_volume": "rffusion-postgres-development-state",
    }
    mounts = {mount["Destination"]: mount for mount in entry["Mounts"]}
    for destination, volume in expected.items():
        if destination not in mounts or mounts[destination].get("Type") != "volume" or mounts[destination].get("Name") != volume:
            raise ValueError(f"Unexpected volume at {destination}")
    if "/RFFusion" not in mounts or mounts["/RFFusion"].get("RW") is not False:
        raise ValueError("Repository must be mounted read-only")
    bindings = entry["HostConfig"]["PortBindings"]
    for key, port in (("5432/tcp", "9083"), ("2828/tcp", "2226")):
        if not any(item["HostPort"] == port for item in bindings.get(key, [])):
            raise ValueError(f"Expected target port mapping absent: {key}")


def main() -> None:
    """Validate input JSON and return nonzero on any missing or inconsistent metadata."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("host", "network", "collisions", "target", "exclusive", "volumes", "baseline"))
    parser.add_argument("--name")
    parser.add_argument("--repo")
    parser.add_argument("--environment", choices=("development", "production"))
    args = parser.parse_args()
    try:
        data = json.load(sys.stdin)
        match args.mode:
            case "host":
                host = data["host"]
                if shutil.disk_usage(data["store"]["graphRoot"]).free < 1024**3:
                    raise ValueError("At least 1 GiB free in Podman graphRoot is required for a fresh installation")
                version = host["cgroupVersion"]
                if version not in ("v1", "v2"):
                    raise ValueError("Unknown cgroup version")
                rootless = host["security"]["rootless"]
                controllers = host["cgroupControllers"]
                if not isinstance(rootless, bool) or not isinstance(controllers, list):
                    raise ValueError("Invalid host capability types")
                print(str(rootless).lower(), version, " ".join(controllers))
            case "network":
                if len(data) != 1 or data[0]["name"] != "podman":
                    raise ValueError("Expected the existing podman network")
                address = ipaddress.ip_address("10.88.0.35")
                if not any(address in ipaddress.ip_network(subnet["subnet"]) for subnet in data[0]["subnets"]):
                    raise ValueError("Static address is outside the configured network")
            case "collisions":
                for entry in data:
                    for network in (entry["NetworkSettings"].get("Networks") or {}).values():
                        if network.get("IPAddress") == "10.88.0.35":
                            raise ValueError("Static IP is already allocated")
                    for bindings in (entry["HostConfig"].get("PortBindings") or {}).values():
                        if any(binding["HostPort"] in ("9083", "2226") for binding in bindings):
                            raise ValueError("Host port already published by another container")
            case "target":
                if not args.name:
                    raise ValueError("Explicit container name is required")
                target(data, args.name)
            case "exclusive":
                for entry in data:
                    if entry["Name"].lstrip("/") == args.name:
                        continue
                    if any(mount.get("Name") in ("rffusion-postgres-development-data", "rffusion-postgres-development-state")
                           for mount in entry["Mounts"]):
                        raise ValueError("Test volumes are referenced by another container")
            case "volumes":
                if args.environment is None:
                    raise ValueError("Explicit volume environment is required")
                names = {f"rffusion-postgres-{args.environment}-{suffix}" for suffix in ("data", "state")}
                if len(data) != 2 or {entry["Name"] for entry in data} != names:
                    raise ValueError("Two named volumes are required")
                for entry in data:
                    labels = entry["Labels"] or {}
                    if (labels.get("io.rffusion.component") != "postgres"
                            or labels.get("io.rffusion.environment") != args.environment
                            or labels.get("io.rffusion.pg-major") != "18"):
                        raise ValueError("Foreign or unrecognized volume identity")
            case "baseline":
                if not args.repo or len(data["sources"]) != 9:
                    raise ValueError("Repository and nine canonical sources are required")
                for source in data["sources"]:
                    path = Path(args.repo) / "src/mariadb/scripts" / source["file"]
                    if hashlib.sha256(path.read_bytes()).hexdigest() != source["sha256"]:
                        raise ValueError("Canonical source changed; re-evaluate the conversion")
    except (KeyError, ValueError, TypeError, OSError) as error:
        raise SystemExit(f"ERROR postgres inspection: {error}") from error


if __name__ == "__main__":
    main()
