#!/opt/conda/envs/appdata/bin/python
"""
Manual helper to emulate the Zabbix -> appCataloga TCP request.

This script intentionally mirrors the payload contract used by:
    /RFFusion/src/zabbix/root/usr/lib/zabbix/externalscripts/queryFileMetadata_trapper.py

It is not part of the automated pytest suite. Its purpose is to make
integration troubleshooting easier while keeping the emitted JSON as close as
possible to the real Zabbix-side request.

Examples:
    /opt/conda/envs/appdata/bin/python /RFFusion/test/tools/emulate_zabbix_appcataloga_request.py \
        "query_tag=backup" \
        "host_id=10806" \
        "host_uid=CWSM211009" \
        "host_add=10.10.10.20" \
        "host_port=2828" \
        "user=celplan" \
        "passwd=secret" \
        "filter_win={\"mode\":\"NONE\",\"start_date\":null,\"end_date\":null,\"last_n_files\":null,\"extension\":\".zip\",\"file_path\":\"C:/CelPlan/CellWireless RU/Spectrum/Completed\",\"file_name\":null}"

    /opt/conda/envs/appdata/bin/python /RFFusion/test/tools/emulate_zabbix_appcataloga_request.py \
        "query_tag=stop" \
        "host_id=10806" \
        "host_uid=CWSM211009" \
        "host_add=10.10.10.20" \
        "host_port=2828" \
        "user=celplan" \
        "passwd=secret" \
        "filter_win={\"mode\":\"RANGE\",\"start_date\":\"2025-01-01\",\"end_date\":\"2025-12-31\",\"last_n_files\":null,\"extension\":\".zip\",\"file_path\":\"C:/CelPlan/CellWireless RU/Spectrum/Completed\",\"file_name\":null}"
"""

from __future__ import annotations

import json
import socket
import sys
from typing import Any


SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5555
SOCKET_TIMEOUT = 30
START_TAG = "<json>"
END_TAG = "</json>"
UTF_ENCODING = "utf-8"

DEFAULTS = {
    "query_tag": "backup",
    "host_id": 0,
    "host_uid": "UNKNOWN",
    "host_add": "127.0.0.1",
    "host_port": 22,
    "user": "root",
    "passwd": "",
    "timeout": SOCKET_TIMEOUT,
    "server_host": SERVER_HOST,
    "server_port": SERVER_PORT,
    "filter_lnx": {
        "mode": "NONE",
        "start_date": None,
        "end_date": None,
        "last_n_files": None,
        "extension": ".bin",
        "file_path": "/mnt/internal",
        "file_name": None,
    },
    "filter_win": {
        "mode": "NONE",
        "start_date": None,
        "end_date": None,
        "last_n_files": None,
        "extension": ".zip",
        "file_path": "C:/CelPlan/CellWireless RU/Spectrum/Completed",
        "file_name": None,
    },
}


def _parse_cli_args(argv: list[str]) -> dict[str, Any]:
    """
    Parse simple ``key=value`` arguments so the call shape stays close to Zabbix.
    """
    parsed = dict(DEFAULTS)

    for raw_arg in argv:
        if "=" not in raw_arg:
            raise ValueError(
                f"Invalid argument '{raw_arg}'. Expected key=value."
            )

        key, value = raw_arg.split("=", 1)
        key = key.strip()
        value = value.strip()

        if key not in parsed:
            raise ValueError(f"Unsupported argument '{key}'.")

        if key in {"host_id", "host_port", "timeout", "server_port"}:
            parsed[key] = int(value)
            continue

        if key in {"filter_lnx", "filter_win"}:
            parsed[key] = _normalize_json(value)
            continue

        parsed[key] = value

    return parsed


def _normalize_json(value: Any) -> Any:
    """
    Normalize JSON passed through shell arguments or copied from Zabbix macros.
    """
    if not isinstance(value, str):
        return value

    normalized = value.strip()

    if (
        (normalized.startswith('"') and normalized.endswith('"'))
        or (normalized.startswith("'") and normalized.endswith("'"))
    ):
        normalized = normalized[1:-1]

    normalized = normalized.replace('\\"', '"')
    return json.loads(normalized)


def _select_filter(args: dict[str, Any]) -> dict[str, Any]:
    """Choose the Windows or Linux filter using the same host-name heuristic as Zabbix."""
    host_uid = str(args["host_uid"] or "")
    return args["filter_win"] if "CW" in host_uid else args["filter_lnx"]


def _build_payload(args: dict[str, Any]) -> dict[str, Any]:
    """Build the exact JSON payload expected by the appCataloga TCP entrypoint."""
    return {
        "query_tag": args["query_tag"],
        "host_id": args["host_id"],
        "host_uid": args["host_uid"],
        "host_add": args["host_add"],
        "host_port": args["host_port"],
        "user": args["user"],
        "passwd": args["passwd"],
        "filter": _select_filter(args),
    }


def _mask_sensitive(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a printable payload copy without exposing the password."""
    safe = dict(payload)
    safe["passwd"] = "*****"
    return safe


def _extract_json_from_response(raw_response: str) -> dict[str, Any]:
    """Extract the framed JSON payload returned by appCataloga."""
    start_index = raw_response.lower().rfind(START_TAG)
    end_index = raw_response.lower().rfind(END_TAG)

    if start_index == -1 or end_index == -1 or end_index <= start_index:
        raise ValueError(f"Framed JSON response not found: {raw_response}")

    json_output = raw_response[start_index + len(START_TAG):end_index]
    return json.loads(json_output)


def main() -> int:
    try:
        args = _parse_cli_args(sys.argv[1:])
        payload = _build_payload(args)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status_query": 0,
                    "message_query": f"Argument error: {exc}",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(args["timeout"])
        client_socket.connect((args["server_host"], args["server_port"]))
        client_socket.sendall(json.dumps(payload).encode(UTF_ENCODING))

        response_bytes = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            response_bytes += chunk

        client_socket.close()
        response_payload = _extract_json_from_response(
            response_bytes.decode(UTF_ENCODING)
        )

        print(
            json.dumps(
                {
                    "status_query": 1,
                    "request": _mask_sensitive(payload),
                    "response": response_payload,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "status_query": 0,
                    "request": _mask_sensitive(payload),
                    "message_query": f"Socket request failed: {exc}",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
