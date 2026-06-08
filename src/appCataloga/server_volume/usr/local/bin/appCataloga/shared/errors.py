"""
Shared error and timeout helpers for appCataloga.

This module centralizes domain-level exceptions, structured error capture, and
small timeout utilities reused across workers and adapters.

It plays three distinct roles in the codebase:
    1. translate noisy runtime failures into stable canonical error codes
    2. classify SSH/SFTP bootstrap failures into retry vs reconcile vs fatal
    3. provide `ErrorHandler`, the lightweight state carrier used by workers
       to capture one workflow failure and persist/log it later

The module is intentionally policy-heavy. Workers call into it when they need
shared operational semantics, not just pretty error strings.
"""

from __future__ import annotations
import re
import socket
import sys
import os
import paramiko
from . import constants, file_utils
from typing import Any, Dict, NamedTuple, Optional, Protocol
from concurrent.futures import TimeoutError as FuturesTimeoutError



# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)


class ErrorEventLogger(Protocol):
    """Minimal logger contract required by `ErrorHandler`."""

    def error_event(self, event: str, **fields: Any) -> None:
        ...


ERROR_CLASSIFIER_VERSION = 1
PERSISTED_ERROR_STAGE_RE = re.compile(r"\[stage=([^\]]+)\]", re.IGNORECASE)
PERSISTED_ERROR_CODE_RE = re.compile(r"\[code=([^\]]+)\]", re.IGNORECASE)
PERSISTED_ERROR_DETAIL_RE = re.compile(r"\[detail=([^\]]+)\]", re.IGNORECASE)
PERSISTED_ERROR_TOKEN_RE = re.compile(r"\[[^\]]+\]")

ERROR_DOMAIN_BY_PREFIX = {
    "backup error": "BACKUP",
    "processing error": "PROCESSING",
    "discovery error": "DISCOVERY",
    "host check error": "HOST_CHECK",
    "backlog management error": "BACKLOG",
}

ERROR_DOMAIN_BY_STAGE = {
    "AUTH": "BACKUP",
    "SSH": "BACKUP",
    "CONNECT": "BACKUP",
    "TRANSFER": "BACKUP",
    "FINALIZE": "BACKUP",
    "LOCK": "BACKUP",
    "HOST_READ": "BACKUP",
    "PROCESS": "PROCESSING",
    "SITE": "PROCESSING",
    "DB": "PROCESSING",
    "FS": "PROCESSING",
    "DISCOVERY": "DISCOVERY",
    "BACKLOG": "BACKLOG",
    "CONNECTIVITY": "HOST_CHECK",
    "UPDATE_STATS": "HOST_CHECK",
    "TRANSACTION": "SYSTEM",
    "QUEUE": "API",
    "HOST_CREATE": "API",
    "COMMAND": "API",
    "PARSE": "API",
    "READ": "API",
    "MAIN": "SYSTEM",
}

ERROR_DOMAIN_BY_CODE = {
    "GPS_GNSS_UNAVAILABLE": "PROCESSING",
    "NO_VALID_SPECTRA": "PROCESSING",
    "SPECTRUM_LIST_EMPTY": "PROCESSING",
    "HOSTNAME_MISSING": "PROCESSING",
    "INVALID_DATETIME_MONTH": "PROCESSING",
    "INVALID_BUFFER_SIZE": "PROCESSING",
    "SITE_GEOGRAPHIC_CODES_NOT_FOUND": "PROCESSING",
    "APP_ANALISE_READ_TIMEOUT": "PROCESSING",
    "APP_ANALISE_TRANSIENT_SERVICE_FAILURE": "PROCESSING",
    "APP_ANALISE_FILE_UNAVAILABLE": "PROCESSING",
    "APP_ANALISE_OUTPUT_ARTIFACT_UNAVAILABLE": "PROCESSING",
    "APP_ANALISE_INVALID_SPECTRA_TYPE": "PROCESSING",
    "APP_ANALISE_ANSWER_ERROR": "PROCESSING",
    "APP_ANALISE_NO_READABLE_FILES_IN_ZIP": "PROCESSING",
    "BIN_PAYLOAD_VALIDATION_FAILED": "PROCESSING",
    "TRANSIENT_FILESYSTEM_FINALIZATION_FAILURE": "PROCESSING",
    "AUTH_FAILED": "BACKUP",
    "SSH_AUTH_TIMEOUT": "BACKUP",
    "SSH_NEGOTIATION_FAILED": "BACKUP",
    "SSH_CONNECT_TIMEOUT": "BACKUP",
    "SFTP_INIT_FAILED": "BACKUP",
    "TRANSFER_TIMEOUT": "BACKUP",
    "TRANSFER_PERMISSION_DENIED": "BACKUP",
    "SSH_TRANSFER_FAILED": "BACKUP",
    "TRANSFER_IO_ERROR": "BACKUP",
    "FILE_TRANSFER_FAILED": "BACKUP",
    "FINALIZE_UPDATE_FAILED": "BACKUP",
    "TASK_LOCK_FAILED": "BACKUP",
    "HOST_NOT_FOUND": "BACKUP",
    "DISCOVERY_FAILED": "DISCOVERY",
    "BACKLOG_PROMOTION_FAILED": "BACKLOG",
    "CONNECTIVITY_CHECK_FAILED": "HOST_CHECK",
    "DB_TRANSACTION_FAILED": "SYSTEM",
    "STATS_UPDATE_FAILED": "HOST_CHECK",
    "HOST_CREATE_FAILED": "API",
    "HOST_TASK_QUEUE_FAILED": "API",
    "EMPTY_REQUEST": "API",
    "UNSUPPORTED_COMMAND": "API",
    "INVALID_HOST_ID": "API",
}


def _canonicalize_error_reason(
    reason: Optional[str],
    exc: Optional[Exception],
    stage: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return `(code, canonical_reason, detail)` for persistence and grouping.

    `canonical_reason` is the stable, aggregation-friendly part of the message.
    `detail` preserves volatile specifics (paths, raw source strings, etc.)
    without forcing dashboards to treat every occurrence as a distinct error.

    The goal is not to preserve the exact original message byte-for-byte. The
    goal is to keep storage and dashboards stable enough that repeated failures
    group together while still retaining the actionable detail operators need.
    """
    raw_reason = (reason or "").strip()
    exc_text = str(exc).strip() if exc is not None else ""

    # -------------------------------------------------------------
    # Generic fallbacks used when the caller supplied little or no
    # structured reason and we have to infer something from `exc`.
    # -------------------------------------------------------------
    if not raw_reason:
        if isinstance(exc, FileNotFoundError) and exc_text:
            return "FILE_NOT_FOUND", "File not found", exc_text
        return None, None, None

    # -------------------------------------------------------------
    # Domain validation errors from BIN / metadata processing.
    # -------------------------------------------------------------
    if "GNSS unavailable sentinel" in raw_reason:
        canonical = "Invalid GPS reading: GNSS unavailable sentinel"
        detail = None

        if raw_reason != canonical:
            # Keep the dashboard/grouping key stable while still preserving the
            # extra context appended by callers such as "all spectra in payload
            # failed GPS validation". When the caller already prefixes the
            # canonical reason, strip that fixed part and retain only the
            # specific suffix as detail.
            if raw_reason.startswith(canonical):
                detail = raw_reason[len(canonical):].lstrip(" |:-") or None
            else:
                detail = raw_reason

        return "GPS_GNSS_UNAVAILABLE", canonical, detail

    if raw_reason == "BIN discarded: no valid spectra after validation":
        return "NO_VALID_SPECTRA", raw_reason, None

    if raw_reason == "Spectrum list is empty":
        return "SPECTRUM_LIST_EMPTY", raw_reason, None

    if raw_reason == "Hostname missing or invalid" or raw_reason.startswith(
        "Hostname resolution failed:"
    ):
        canonical = "Hostname missing or invalid"
        detail = raw_reason if raw_reason != canonical else None
        return "HOSTNAME_MISSING", canonical, detail

    if isinstance(exc, KeyError) and raw_reason in {"'hostname'", '"hostname"'}:
        return "HOSTNAME_MISSING", "Hostname missing or invalid", raw_reason

    if isinstance(exc, FileNotFoundError):
        return "FILE_NOT_FOUND", "File not found", raw_reason

    if raw_reason.startswith('Month out of range in datetime string "'):
        return "INVALID_DATETIME_MONTH", "Invalid datetime string: month out of range", raw_reason

    if raw_reason == "buffer size must be a multiple of element size":
        return "INVALID_BUFFER_SIZE", "Invalid binary buffer size", raw_reason

    if raw_reason == "APP_ANALISE read timeout during processing":
        return (
            "APP_ANALISE_READ_TIMEOUT",
            "APP_ANALISE read timeout during processing",
            exc_text or None,
        )

    if raw_reason == "Transient appAnalise processing failure":
        return (
            "APP_ANALISE_TRANSIENT_SERVICE_FAILURE",
            "Transient appAnalise processing failure",
            exc_text or None,
        )

    if (
        raw_reason == "APP_ANALISE file unavailable during processing"
        and isinstance(exc, AppAnaliseFileUnavailableError)
    ):
        if exc_text.startswith("APP_ANALISE output artifact unavailable:"):
            detail = exc_text.replace("[", "(").replace("]", ")")
            return (
                "APP_ANALISE_OUTPUT_ARTIFACT_UNAVAILABLE",
                "APP_ANALISE output artifact unavailable",
                detail,
            )

        return (
            "APP_ANALISE_FILE_UNAVAILABLE",
            "APP_ANALISE file unavailable during processing",
            exc_text or None,
        )

    if raw_reason.startswith("APP_ANALISE source file unavailable before request:"):
        return (
            "APP_ANALISE_FILE_UNAVAILABLE",
            "APP_ANALISE source file unavailable before request",
            raw_reason,
        )

    if raw_reason.startswith(
        "APP_ANALISE reported missing source file and it is absent locally:"
    ):
        return (
            "APP_ANALISE_FILE_UNAVAILABLE",
            "APP_ANALISE source file unavailable",
            raw_reason,
        )

    if raw_reason == "Transient filesystem finalization failure":
        return (
            "TRANSIENT_FILESYSTEM_FINALIZATION_FAILURE",
            "Transient filesystem finalization failure",
            exc_text or None,
        )

    if raw_reason.startswith("APP_ANALISE returned invalid Answer.Spectra type"):
        canonical = "APP_ANALISE returned invalid Answer.Spectra type"
        detail = raw_reason if raw_reason != canonical else None
        return "APP_ANALISE_INVALID_SPECTRA_TYPE", canonical, detail

    if raw_reason.startswith("APP_ANALISE returned error in Answer:"):
        detail = raw_reason.split("APP_ANALISE returned error in Answer:", 1)[1].strip()

        if detail == "model:SpecDataBase:NoReadableFilesInZip":
            return (
                "APP_ANALISE_NO_READABLE_FILES_IN_ZIP",
                "APP_ANALISE reported no readable files in ZIP",
                detail,
            )

        return (
            "APP_ANALISE_ANSWER_ERROR",
            "APP_ANALISE returned error in Answer",
            detail or None,
        )

    if (
        raw_reason == "Payload validation failed during processing"
        and isinstance(exc, BinValidationError)
    ):
        if exc_text and exc_text != raw_reason:
            nested_code, nested_reason, nested_detail = _canonicalize_error_reason(
                exc_text,
                None,
                stage=stage,
            )
            if nested_code:
                return nested_code, nested_reason, nested_detail

        return (
            "BIN_PAYLOAD_VALIDATION_FAILED",
            "Payload validation failed during processing",
            exc_text or None,
        )

    # -------------------------------------------------------------
    # DIM / enrichment failures where the human-readable message is
    # too specific to use directly as the dashboard grouping key.
    # -------------------------------------------------------------
    if (
        raw_reason.startswith("Error inserting site in DIM_SPECTRUM_SITE:")
        and "Error retrieving geographic codes:" in raw_reason
    ):
        return (
            "SITE_GEOGRAPHIC_CODES_NOT_FOUND",
            "Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found",
            raw_reason,
        )

    # Backup and discovery workers often use stable generic reasons and rely on
    # the exception object for the actionable detail. Canonicalize these cases
    # so dashboards can aggregate by code without discarding what Paramiko/OS
    # actually reported.
    if stage == "AUTH" or isinstance(exc, paramiko.AuthenticationException):
        if exc is not None and is_auth_timeout_error(exc):
            detail = exc_text or (
                raw_reason
                if raw_reason != "SSH authentication failed"
                else None
            )
            return "SSH_AUTH_TIMEOUT", "SSH authentication timed out", detail

        detail = exc_text or (
            raw_reason
            if raw_reason not in {
                "Authentication failed (bad credentials)",
                "SSH authentication failed",
            }
            else None
        )
        return "AUTH_FAILED", "Authentication failed", detail

    if stage == "SSH" or raw_reason == "SSH negotiation failed":
        detail = exc_text or (
            raw_reason if raw_reason != "SSH negotiation failed" else None
        )
        return "SSH_NEGOTIATION_FAILED", "SSH negotiation failed", detail

    if stage == "CONNECT":
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return "SSH_CONNECT_TIMEOUT", "SSH/SFTP connection timed out", exc_text or raw_reason

        detail = exc_text or (
            raw_reason if raw_reason != "SSH/SFTP initialization failed" else None
        )
        return "SFTP_INIT_FAILED", "SSH/SFTP initialization failed", detail

    # -------------------------------------------------------------
    # Transfer is the noisiest worker stage: the same outer reason
    # may wrap source drift, SSH transport issues, or local I/O.
    # -------------------------------------------------------------
    if stage == "TRANSFER":
        if isinstance(exc, FileNotFoundError):
            return "FILE_NOT_FOUND", "File not found", exc_text or raw_reason

        if isinstance(exc, (socket.timeout, TimeoutError)):
            return "TRANSFER_TIMEOUT", "File transfer timed out", exc_text or raw_reason

        if isinstance(exc, PermissionError):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "TRANSFER_PERMISSION_DENIED", "Permission denied during transfer", detail

        if isinstance(exc, paramiko.SSHException):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "SSH_TRANSFER_FAILED", "SSH/SFTP transfer failed", detail

        if isinstance(exc, OSError):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "TRANSFER_IO_ERROR", "Filesystem error during transfer", detail

        if raw_reason == "File transfer failed":
            return "FILE_TRANSFER_FAILED", raw_reason, exc_text or None

    if raw_reason == "Failed to lock HOST or FILE_TASK":
        return "TASK_LOCK_FAILED", raw_reason, exc_text or None

    if raw_reason in {"Failed to lock HOST or HOST_TASK", "Failed to lock task"}:
        canonical = "Failed to lock task"
        detail = raw_reason if raw_reason != canonical else None
        if exc_text:
            detail = exc_text
        return "TASK_LOCK_FAILED", canonical, detail

    if raw_reason == "Host not found in database":
        return "HOST_NOT_FOUND", raw_reason, None

    if raw_reason == "Post-transfer update failed":
        return "FINALIZE_UPDATE_FAILED", raw_reason, exc_text or None

    # -------------------------------------------------------------
    # Stable stage-level reasons used by workers and service entrypoints.
    # -------------------------------------------------------------
    if stage == "DISCOVERY" or raw_reason == "Discovery failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Discovery failed" else None
        )
        return "DISCOVERY_FAILED", "Discovery failed", detail

    if stage == "BACKLOG" or raw_reason == "Backlog promotion failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Backlog promotion failed" else None
        )
        return "BACKLOG_PROMOTION_FAILED", "Backlog promotion failed", detail

    if stage == "CONNECTIVITY" or raw_reason == "Connectivity test failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Connectivity test failed" else None
        )
        return "CONNECTIVITY_CHECK_FAILED", "Connectivity test failed", detail

    if stage == "TRANSACTION" or raw_reason == "DB transaction failed":
        detail = exc_text or (
            raw_reason if raw_reason != "DB transaction failed" else None
        )
        return "DB_TRANSACTION_FAILED", "DB transaction failed", detail

    if stage == "UPDATE_STATS" or raw_reason == "Statistics update failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Statistics update failed" else None
        )
        return "STATS_UPDATE_FAILED", "Statistics update failed", detail

    if stage == "HOST_CREATE" or raw_reason == "Failed to create/ensure HOST":
        detail = exc_text or (
            raw_reason if raw_reason != "Failed to create/ensure HOST" else None
        )
        return "HOST_CREATE_FAILED", "Failed to create/ensure HOST", detail

    if stage == "QUEUE" or raw_reason == "Failed to queue HOST_TASK":
        detail = exc_text or (
            raw_reason if raw_reason != "Failed to queue HOST_TASK" else None
        )
        return "HOST_TASK_QUEUE_FAILED", "Failed to queue HOST_TASK", detail

    if stage == "READ" and raw_reason == "Empty request":
        return "EMPTY_REQUEST", "Empty request", None

    if stage == "COMMAND" and raw_reason == "Unsupported command":
        return "UNSUPPORTED_COMMAND", "Unsupported command", None

    if stage == "PARSE" and raw_reason == "Invalid host_id":
        return "INVALID_HOST_ID", "Invalid host_id", None

    return None, raw_reason, None


def empty_persisted_error_fields(*, classified: bool = False) -> Dict[str, Any]:
    """
    Return the structured FILE_TASK / FILE_TASK_HISTORY error columns.

    `classified=True` lets callers explicitly clear stale error metadata when a
    row transitions back to a non-error state.
    """
    return {
        "NA_ERROR_DOMAIN": None,
        "NA_ERROR_STAGE": None,
        "NA_ERROR_CODE": None,
        "NA_ERROR_SUMMARY": None,
        "NA_ERROR_DETAIL": None,
        "NU_ERROR_CLASSIFIER_VERSION": (
            ERROR_CLASSIFIER_VERSION if classified else None
        ),
    }


def _infer_error_domain(
    *,
    prefix: Optional[str],
    stage: Optional[str],
    code: Optional[str],
) -> Optional[str]:
    """Infer a coarse error domain from the message wrapper and parsed tags."""
    normalized_prefix = (prefix or "").split("|", 1)[0].strip().lower()

    if normalized_prefix in ERROR_DOMAIN_BY_PREFIX:
        return ERROR_DOMAIN_BY_PREFIX[normalized_prefix]

    if code:
        normalized_code = str(code).strip().upper()
        if normalized_code in ERROR_DOMAIN_BY_CODE:
            return ERROR_DOMAIN_BY_CODE[normalized_code]

    if stage:
        normalized_stage = str(stage).strip().upper()
        if normalized_stage in ERROR_DOMAIN_BY_STAGE:
            return ERROR_DOMAIN_BY_STAGE[normalized_stage]

    return None


def _extract_embedded_error_fragment(message: str) -> tuple[str, Optional[str]]:
    """
    Split a persisted audit message into `(prefix, error_fragment)`.

    Non-error task messages return `(prefix, None)`.
    """
    normalized = (message or "").strip()

    if not normalized:
        return "", None

    if "[ERROR]" in normalized:
        prefix, fragment = normalized.split("[ERROR]", 1)
        return prefix.strip(" |"), fragment.strip()

    lowered = normalized.lower()
    looks_structured = "[stage=" in lowered or "[code=" in lowered
    has_error_prefix = any(prefix in lowered for prefix in ERROR_DOMAIN_BY_PREFIX)

    if looks_structured or has_error_prefix:
        return "", normalized

    return normalized, None


def _extract_error_summary(error_fragment: str) -> Optional[str]:
    """Strip bracketed tokens and keep only the stable human summary."""
    if not error_fragment:
        return None

    summary = PERSISTED_ERROR_TOKEN_RE.sub(" ", error_fragment)
    summary = re.sub(r"\s+", " ", summary).strip(" |")
    return summary or None


def _render_persisted_error_fragment(
    *,
    stage: Optional[str],
    code: Optional[str],
    summary: Optional[str],
    detail: Optional[str],
) -> str:
    """Build the compact structured error fragment used in persisted messages."""
    parts = ["[ERROR]"]

    if stage:
        parts.append(f"[stage={stage}]")

    if code:
        parts.append(f"[code={code}]")

    if summary:
        parts.append(summary)

    if detail:
        parts.append(f"[detail={detail}]")

    return " ".join(parts)


def canonicalize_persisted_error_message(message: Optional[str]) -> Optional[str]:
    """
    Normalize one persisted audit message into a compact canonical form.

    This keeps `NA_MESSAGE` readable for operators while stripping volatile
    tokens such as exception types and per-attempt IDs that already live in
    dedicated columns. Non-error messages pass through unchanged.
    """
    normalized = (message or "").strip()

    if not normalized:
        return message

    prefix, error_fragment = _extract_embedded_error_fragment(normalized)

    if error_fragment is None:
        return normalized

    payload = classify_persisted_error_message(normalized)

    rendered_error = _render_persisted_error_fragment(
        stage=payload.get("NA_ERROR_STAGE"),
        code=payload.get("NA_ERROR_CODE"),
        summary=payload.get("NA_ERROR_SUMMARY"),
        detail=payload.get("NA_ERROR_DETAIL"),
    )

    normalized_prefix = prefix.strip(" |")
    if normalized_prefix:
        return f"{normalized_prefix} | {rendered_error}"

    if "[ERROR]" in normalized:
        return rendered_error

    leading_label = normalized.split("|", 1)[0].strip()
    if leading_label and leading_label.lower() in ERROR_DOMAIN_BY_PREFIX:
        return f"{leading_label} | {rendered_error}"

    return rendered_error


def classify_persisted_error_message(message: Optional[str]) -> Dict[str, Any]:
    """
    Parse one persisted task/history message into structured error columns.

    The parser accepts both the current `ErrorHandler.format_error()` output and
    older task messages that already contained partial `[stage=...]` / `[code=...]`
    markup inside `NA_MESSAGE`.
    """
    normalized = (message or "").strip()

    if not normalized:
        return empty_persisted_error_fields(classified=False)

    prefix, error_fragment = _extract_embedded_error_fragment(normalized)
    payload = empty_persisted_error_fields(classified=True)

    if error_fragment is None:
        return payload

    stage_match = PERSISTED_ERROR_STAGE_RE.search(error_fragment)
    code_match = PERSISTED_ERROR_CODE_RE.search(error_fragment)
    detail_match = PERSISTED_ERROR_DETAIL_RE.search(error_fragment)

    stage = stage_match.group(1).strip() if stage_match else None
    code = code_match.group(1).strip() if code_match else None
    detail = detail_match.group(1).strip() if detail_match else None
    summary = _extract_error_summary(error_fragment)

    fallback_code = None
    fallback_summary = None
    fallback_detail = None

    if summary or stage:
        fallback_code, fallback_summary, fallback_detail = _canonicalize_error_reason(
            summary or normalized,
            None,
            stage=stage,
        )

    if not code and fallback_code:
        code = fallback_code

    if fallback_summary and (not summary or summary == normalized):
        summary = fallback_summary

    if not detail and fallback_detail:
        detail = fallback_detail

    if detail:
        refined_code, refined_summary, refined_detail = _canonicalize_error_reason(
            detail,
            None,
            stage=stage,
        )
        if refined_code and code in {
            None,
            "UNCLASSIFIED",
            "BIN_PAYLOAD_VALIDATION_FAILED",
            "APP_ANALISE_ANSWER_ERROR",
        }:
            code = refined_code
            if refined_summary:
                summary = refined_summary
            if refined_detail is not None:
                detail = refined_detail

    if not code and (stage or summary or prefix):
        code = "UNCLASSIFIED"

    if not summary:
        summary = prefix or normalized

    payload.update(
        {
            "NA_ERROR_DOMAIN": _infer_error_domain(
                prefix=prefix,
                stage=stage,
                code=code,
            ) or "UNKNOWN",
            "NA_ERROR_STAGE": stage,
            "NA_ERROR_CODE": code,
            "NA_ERROR_SUMMARY": summary,
            "NA_ERROR_DETAIL": detail,
        }
    )
    return payload


def persisted_error_fields_from_handler(
    handler: Optional["ErrorHandler"] = None,
    *,
    message: Optional[str] = None,
    clear_when_empty: bool = True,
) -> Dict[str, Any]:
    """
    Build the structured FILE_TASK / FILE_TASK_HISTORY error columns.

    Workers should persist these fields explicitly so the row already carries
    the canonical error payload before any downstream aggregation reads it.
    """
    if handler is not None:
        triggered = getattr(handler, "triggered", None)
        format_persisted_error = getattr(handler, "format_persisted_error", None)
        format_error = getattr(handler, "format_error", None)

        if triggered:
            if callable(format_persisted_error):
                return classify_persisted_error_message(
                    handler.format_persisted_error()
                )
            return classify_persisted_error_message(handler.format_error())

        if triggered is None and callable(format_error):
            formatted = format_error()
            if formatted:
                return classify_persisted_error_message(formatted)

    if message is not None:
        return classify_persisted_error_message(message)

    return empty_persisted_error_fields(classified=clear_when_empty)

class BinValidationError(ValueError):
    """
    Raised when BIN semantic validation fails.

    This is a domain-level fatal validation error. Retrying the same payload
    without changing its contents will not make it valid.
    """
    pass


class ExternalServiceTransientError(Exception):
    """
    Raised when an external dependency fails transiently.

    These errors should not be interpreted as proof that the source
    file is invalid, because a retry may succeed once the dependency
    becomes healthy again.
    """
    pass


class AppAnaliseFileUnavailableError(Exception):
    """
    Raised when appAnalise cannot see or produce a concrete file artifact.

    This is intentionally separate from broad transport outages: an unreachable
    socket, a read timeout, and a missing output `.mat` require different
    operator action even though they may all freeze the queue row.
    """
    pass


class AppAnaliseReadTimeoutError(Exception):
    """
    Raised when appAnalise returns a structured FileRead timeout.

    This is not the same as a transport/socket timeout from RF.Fusion's point
    of view: the external service stayed responsive enough to answer, but it
    could not finish this specific payload inside its own execution budget.
    """
    pass


PROCESSING_FREEZE_DETAILS = {
    AppAnaliseReadTimeoutError: "APP_ANALISE read timeout, task frozen for manual review",
    AppAnaliseFileUnavailableError: "APP_ANALISE file unavailable, task frozen for manual review",
    ExternalServiceTransientError: "Transient appAnalise service failure, task frozen for manual review",
}


def should_freeze_processing_task(exc: BaseException | None) -> bool:
    """Return whether one processing failure should freeze the task."""
    return (
        isinstance(exc, tuple(PROCESSING_FREEZE_DETAILS.keys()))
        or file_utils.is_transient_filesystem_error(exc)
    )


def freeze_processing_detail(exc: BaseException | None) -> str:
    """Return the operator-facing freeze detail for one processing failure."""
    if exc is not None:
        detail = PROCESSING_FREEZE_DETAILS.get(type(exc))
        if detail:
            return detail
        if file_utils.is_transient_filesystem_error(exc):
            return "Transient filesystem finalization failure, task frozen for manual review"
    return "Task frozen for manual review"


AUTH_TIMEOUT_MESSAGE_SNIPPETS = (
    "authentication timeout",
    "auth timeout",
)


def is_auth_timeout_error(exc: Exception) -> bool:
    """
    Return whether a Paramiko authentication failure is timeout-driven.

    Some hosts reach the authentication phase but answer too slowly for a short
    probe. Those cases should be treated as timeout/degraded, not as explicit
    bad credentials.
    """
    if not isinstance(exc, paramiko.AuthenticationException):
        return False

    normalized = str(exc).strip().lower()
    return any(snippet in normalized for snippet in AUTH_TIMEOUT_MESSAGE_SNIPPETS)


class SshConnectClass(NamedTuple):
    """Classification result for any SSH connection-time exception."""

    state: str        # k.HOST_CONN_* value used by connectivity probe results
    reason: str       # short slug used in log events and probe results
    stage: str        # k.STAGE_* value used by workers for err.capture
    ssh_online: bool  # True when the TCP/SSH layer was reached before failure


def _is_ssh_connect_exception(exc: Exception) -> bool:
    """Return whether `exc` looks like an SSH bootstrap failure."""
    return isinstance(
        exc,
        (
            paramiko.AuthenticationException,
            paramiko.SSHException,
            paramiko.ssh_exception.NoValidConnectionsError,
            socket.timeout,
            TimeoutError,
            OSError,
        ),
    )


def classify_ssh_connect_exc(exc: Exception) -> SshConnectClass:
    """
    Classify any SSH connect-time exception into a stable four-field descriptor.

    This is the single shared classifier used by both ssh_probe (which builds
    probe payloads) and workers (which need the right stage for err.capture).
    Keeping both callers anchored to the same function prevents exception
    handling logic from drifting apart over time.
    """
    if isinstance(exc, paramiko.AuthenticationException):
        if is_auth_timeout_error(exc):
            return SshConnectClass(
                state=k.HOST_CONN_DEGRADED,
                reason="ssh_auth_timeout",
                stage=k.STAGE_AUTH,
                ssh_online=True,
            )
        return SshConnectClass(
            state=k.HOST_CONN_AUTH_ERROR,
            reason="ssh_auth_failed",
            stage=k.STAGE_AUTH,
            ssh_online=True,
        )

    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        return SshConnectClass(
            state=k.HOST_CONN_DEGRADED,
            reason="ssh_no_valid_connections",
            stage=k.STAGE_CONNECT,
            ssh_online=False,
        )

    reason = (
        "ssh_timeout"
        if isinstance(exc, (socket.timeout, TimeoutError))
        else "ssh_unreachable"
    )
    return SshConnectClass(
        state=k.HOST_CONN_DEGRADED,
        reason=reason,
        stage=k.STAGE_CONNECT,
        ssh_online=False,
    )


def classify_ssh_connect_failure(exc: Exception) -> tuple[str, str] | None:
    """
    Return canonical `(reason, stage)` for worker-level SSH bootstrap failures.

    Workers use this helper only when they need to translate a raw exception
    into `err.capture(...)` fields. Non-SSH exceptions return `None` so the
    worker can apply its local fallback without misclassifying DB or queue
    failures as transport errors.
    """
    if not _is_ssh_connect_exception(exc):
        return None

    classification = classify_ssh_connect_exc(exc)
    match classification.reason:
        case "ssh_auth_timeout":
            return "SSH authentication timed out", classification.stage
        case "ssh_auth_failed":
            return "SSH authentication failed", classification.stage
        case "ssh_no_valid_connections":
            return "SSH connection failed: no valid connections", classification.stage
        case "ssh_timeout":
            return "SSH connection timed out", classification.stage
        case _:
            return "SSH connection failed", classification.stage

class ErrorHandler:
    """
    Centralized error tracking helper for long-running services.

    The handler stores the first meaningful failure in a workflow and exposes
    helpers to log or persist that failure later, typically in `finally`
    blocks or broad exception boundaries.

    Usage:
        err = ErrorHandler(log)
        err.set("Discovery failed", stage="DISCOVERY", exc=e)

        if err.triggered:
            err.log_error(host_id=..., task_id=...)

    The handler is intentionally simple:
        - capture the first meaningful failure
        - keep structured context next to it
        - let the caller decide later whether to log, persist, or both
    """

    def __init__(self, log: "ErrorEventLogger"):
        self.logger = log
        self.reason = None
        self.stage = None
        self.exc = None
        self.context: Dict[str, Any] = {}

    def set(
        self,
        reason: str,
        stage: str = None,
        exc: Exception = None,
        **context: Any,
    ):
        """
        Register the first meaningful error and ignore later noise.

        Workers often cross several cleanup branches after the original
        failure. Preserving only the first failure keeps persistence stable and
        prevents secondary cleanup noise from overwriting the root cause.
        """
        if not self.reason:
            self.reason = reason
            self.stage = stage
            self.exc = exc
            self.context = {
                str(key): value
                for key, value in context.items()
                if value is not None
            }

    def capture(
        self,
        reason: str,
        stage: str = None,
        exc: Exception = None,
        **context: Any,
    ):
        """
        Alias for `set()` used at exception boundaries.

        The shorter name reads better inside `except` blocks and staged worker
        pipelines, where most callers use this helper.
        """
        self.set(reason=reason, stage=stage, exc=exc, **context)

    @property
    def triggered(self) -> bool:
        return self.reason is not None

    @property
    def msg(self) -> str:
        """Return a compact stage-prefixed message for quick human reads."""
        if self.stage:
            return f"{self.stage}: {self.reason}"
        return self.reason or ""

    def log_error(self, **runtime_context: Any):
        """
        Emit one structured error log enriched with stored and runtime context.

        `self.context` holds the facts captured at the failure point. The
        optional `runtime_context` lets callers attach outer-loop information
        only available at log time, such as traceback or aggregate counters.
        """
        merged_context = dict(self.context)
        for key, value in runtime_context.items():
            if value is not None:
                merged_context[str(key)] = value

        payload = {
            "component": "shared_errors",
            "operation": "log_error",
            "stage": self.stage,
            "reason": self.reason or "Unknown error",
            "error_type": type(self.exc).__name__ if self.exc else "Unknown",
        }
        error_code, _, detail = _canonicalize_error_reason(
            self.reason,
            self.exc,
            stage=self.stage,
        )
        if error_code:
            payload["error_code"] = error_code
        if detail:
            payload["error_detail"] = detail
        payload.update(merged_context)

        if self.exc is not None:
            payload["exception"] = repr(self.exc)

        self.logger.error_event("error_handler_triggered", **payload)

    def _format_structured_error(
        self,
        *,
        include_type: bool,
        include_context: bool,
    ) -> str:
        """Render one structured error string with configurable verbosity."""
        if not self.triggered:
            return ""

        exc_type = type(self.exc).__name__ if self.exc else "Unknown"
        error_code, canonical_reason, detail = _canonicalize_error_reason(
            self.reason,
            self.exc,
            stage=self.stage,
        )

        parts = ["[ERROR]"]

        if self.stage:
            parts.append(f"[stage={self.stage}]")

        if include_type:
            parts.append(f"[type={exc_type}]")

        if error_code:
            parts.append(f"[code={error_code}]")

        if canonical_reason:
            parts.append(canonical_reason)

        if detail:
            parts.append(f"[detail={detail}]")

        if include_context and self.context:
            parts.extend(
                [f"[{key}={value}]" for key, value in self.context.items()]
            )

        return " ".join(parts)

    def format_error(self) -> str:
        """
        Return the richer structured error string used in logs and APIs.

        This flavor keeps exception type and captured runtime context because
        those channels value immediate operator detail over storage stability.
        """
        return self._format_structured_error(
            include_type=True,
            include_context=True,
        )

    def format_persisted_error(self) -> str:
        """
        Return the compact structured error string used in DB audit fields.

        Persistence intentionally omits volatile tokens such as exception type
        and per-attempt context (`host_id`, `task_id`, etc.) because those
        facts already live in dedicated columns and only make `NA_MESSAGE`
        noisier and less groupable.
        """
        return self._format_structured_error(
            include_type=False,
            include_context=False,
        )



class TimeoutError(Exception):
    """
    Raised when a function exceeds the allowed timeout.

    This is the module-local timeout abstraction returned by `run_with_timeout`
    so callers do not need to know about `concurrent.futures`.
    """
    pass

def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using the shared executor from `constants`.

    This helper keeps the rest of the codebase independent from executor
    details. Callers supply a zero-argument function and get either:
        - the result
        - `TimeoutError`
        - the original exception raised by `func`

    Raises:
        TimeoutError
        Exception forwarded from func()
    """
    future = constants._TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception as e:
        raise e
