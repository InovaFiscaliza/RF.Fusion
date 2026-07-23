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
import os
import re
import socket
import sys
import builtins
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any, Callable, Dict, NamedTuple, Optional, Protocol

import paramiko

from . import constants, file_utils



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


ErrorTriple = tuple[Optional[str], Optional[str], Optional[str]]

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
    k.STAGE_AUTH: "BACKUP",
    k.STAGE_SSH: "BACKUP",
    k.STAGE_CONNECT: "BACKUP",
    k.STAGE_TRANSFER: "BACKUP",
    k.STAGE_FINALIZE: "BACKUP",
    k.STAGE_LOCK: "BACKUP",
    k.STAGE_HOST_READ: "BACKUP",
    k.STAGE_PROCESS: "PROCESSING",
    k.STAGE_SITE: "PROCESSING",
    k.STAGE_DB: "PROCESSING",
    k.STAGE_FS: "PROCESSING",
    k.STAGE_DISCOVERY: "DISCOVERY",
    k.STAGE_BACKLOG: "BACKLOG",
    k.STAGE_CONNECTIVITY: "HOST_CHECK",
    k.STAGE_UPDATE_STATS: "HOST_CHECK",
    k.STAGE_TRANSACTION: "SYSTEM",
    k.STAGE_QUEUE: "API",
    k.STAGE_HOST_CREATE: "API",
    k.STAGE_COMMAND: "API",
    k.STAGE_PARSE: "API",
    k.STAGE_READ: "API",
    k.STAGE_MAIN: "SYSTEM",
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
    "APP_ANALISE_SERVICE_RESPONSE_ERROR": "PROCESSING",
    "APP_ANALISE_EMPTY_SPEC_DATA": "PROCESSING",
    "APP_ANALISE_NO_SPECTRAL_DATA": "PROCESSING",
    "APP_ANALISE_FILE_UNAVAILABLE": "PROCESSING",
    "APP_ANALISE_OUTPUT_ARTIFACT_UNAVAILABLE": "PROCESSING",
    "APP_ANALISE_INVALID_SUCCESS_PAYLOAD": "PROCESSING",
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

APP_ANALISE_ANSWER_CLASSIFICATIONS = {
    k.APP_ANALISE_NO_READABLE_FILES_IN_ZIP_DETAIL: (
        "APP_ANALISE_NO_READABLE_FILES_IN_ZIP",
        "APP_ANALISE reported no readable files in ZIP",
    ),
    k.APP_ANALISE_EMPTY_SPEC_DATA_DETAIL: (
        "APP_ANALISE_EMPTY_SPEC_DATA",
        "APP_ANALISE returned empty spectrum data",
    ),
    k.APP_ANALISE_NO_SPECTRAL_DATA_DETAIL: (
        "APP_ANALISE_NO_SPECTRAL_DATA",
        "APP_ANALISE reported no spectral data",
    ),
}

AUTH_FAILED_DETAIL_EXCEPTIONS = {
    "Authentication failed (bad credentials)",
    "SSH authentication failed",
}

SSH_AUTH_FAILED_REASON = "SSH authentication failed"
SSH_NEGOTIATION_FAILED_REASON = "SSH negotiation failed"
SFTP_INIT_FAILED_REASON = "SSH/SFTP initialization failed"
FILE_TRANSFER_FAILED_REASON = "File transfer failed"
DISCOVERY_FAILED_REASON = "Discovery failed"
BACKLOG_PROMOTION_FAILED_REASON = "Backlog promotion failed"
CONNECTIVITY_TEST_FAILED_REASON = "Connectivity test failed"
DB_TRANSACTION_FAILED_REASON = "DB transaction failed"
STATS_UPDATE_FAILED_REASON = "Statistics update failed"
HOST_CREATE_FAILED_REASON = "Failed to create/ensure HOST"
HOST_TASK_QUEUE_FAILED_REASON = "Failed to queue HOST_TASK"
EMPTY_REQUEST_REASON = "Empty request"
UNSUPPORTED_COMMAND_REASON = "Unsupported command"
INVALID_HOST_ID_REASON = "Invalid host_id"


def _detail_from_reason(
    raw_reason: str,
    canonical_reason: str,
) -> Optional[str]:
    """Return the original reason only when it adds information."""
    if raw_reason == canonical_reason:
        return None
    return raw_reason


def _prefer_exc_text(
    raw_reason: str,
    exc_text: str,
    *,
    ignored_reasons: set[str] | None = None,
) -> Optional[str]:
    """Prefer exception detail and fall back to the outer reason when useful."""
    if exc_text:
        return exc_text
    if ignored_reasons and raw_reason in ignored_reasons:
        return None
    return raw_reason or None


def _is_timeout_exception(exc: BaseException | None) -> bool:
    """Return whether one exception represents a timeout condition."""
    return isinstance(
        exc,
        (
            socket.timeout,
            builtins.TimeoutError,
            OperationTimeoutError,
        ),
    )


def _classify_app_analise_answer_detail(detail: str) -> ErrorTriple:
    """Return one canonical error triple for a known appAnalise answer detail."""
    match = APP_ANALISE_ANSWER_CLASSIFICATIONS.get(detail)
    if match is None:
        return None, None, None

    code, summary = match
    return code, summary, detail


def _classify_empty_reason(exc: Optional[Exception], exc_text: str) -> ErrorTriple:
    """Handle the sparse cases where only the exception carries meaning."""
    if isinstance(exc, FileNotFoundError) and exc_text:
        return "FILE_NOT_FOUND", "File not found", exc_text
    return None, None, None


def _classify_validation_error(
    raw_reason: str,
    exc: Optional[Exception],
    exc_text: str,
    *,
    stage: Optional[str],
) -> ErrorTriple:
    """Classify BIN validation and payload-shape failures."""
    if "GNSS unavailable sentinel" in raw_reason:
        canonical = "Invalid GPS reading: GNSS unavailable sentinel"
        detail = None

        if raw_reason != canonical:
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
        return "HOSTNAME_MISSING", canonical, _detail_from_reason(
            raw_reason,
            canonical,
        )

    if isinstance(exc, KeyError) and raw_reason in {"'hostname'", '"hostname"'}:
        return "HOSTNAME_MISSING", "Hostname missing or invalid", raw_reason

    if isinstance(exc, FileNotFoundError):
        return "FILE_NOT_FOUND", "File not found", raw_reason

    if raw_reason.startswith('Month out of range in datetime string "'):
        return (
            "INVALID_DATETIME_MONTH",
            "Invalid datetime string: month out of range",
            raw_reason,
        )

    if raw_reason == "buffer size must be a multiple of element size":
        return "INVALID_BUFFER_SIZE", "Invalid binary buffer size", raw_reason

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

    return None, None, None


def _classify_app_analise_error(
    raw_reason: str,
    exc: Optional[Exception],
    exc_text: str,
    *,
    stage: Optional[str] = None,
) -> ErrorTriple:
    """Classify appAnalise processing and response failures."""
    _ = stage

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

    if raw_reason == "APP_ANALISE service returned processing error":
        if exc_text:
            detail = exc_text.split(
                "APP_ANALISE returned error in Answer:",
                1,
            )[-1].strip()
            code, canonical_reason, canonical_detail = (
                _classify_app_analise_answer_detail(detail)
            )
            if code:
                return code, canonical_reason, canonical_detail
        return (
            "APP_ANALISE_SERVICE_RESPONSE_ERROR",
            "APP_ANALISE service returned processing error",
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
        return "APP_ANALISE_INVALID_SPECTRA_TYPE", canonical, _detail_from_reason(
            raw_reason,
            canonical,
        )

    if raw_reason == "APP_ANALISE returned invalid success payload":
        return (
            "APP_ANALISE_INVALID_SUCCESS_PAYLOAD",
            "APP_ANALISE returned invalid success payload",
            exc_text or None,
        )

    if raw_reason.startswith("APP_ANALISE returned error in Answer:"):
        detail = raw_reason.split("APP_ANALISE returned error in Answer:", 1)[1].strip()
        code, canonical_reason, canonical_detail = _classify_app_analise_answer_detail(
            detail
        )
        if code:
            return code, canonical_reason, canonical_detail

        return (
            "APP_ANALISE_ANSWER_ERROR",
            "APP_ANALISE returned error in Answer",
            detail or None,
        )

    return None, None, None


def _classify_site_error(
    raw_reason: str,
    exc: Optional[Exception],
    exc_text: str,
    *,
    stage: Optional[str] = None,
) -> ErrorTriple:
    """Classify site-enrichment failures with unstable human detail."""
    _ = (exc, exc_text, stage)

    if (
        raw_reason.startswith("Error inserting site in DIM_SPECTRUM_SITE:")
        and "Error retrieving geographic codes:" in raw_reason
    ):
        return (
            "SITE_GEOGRAPHIC_CODES_NOT_FOUND",
            "Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found",
            raw_reason,
        )
    return None, None, None


def _classify_backup_stage_error(
    raw_reason: str,
    exc: Optional[Exception],
    exc_text: str,
    *,
    stage: Optional[str],
) -> ErrorTriple:
    """Classify backup and SSH failures that depend on stage and exception."""
    if stage == k.STAGE_AUTH or isinstance(exc, paramiko.AuthenticationException):
        if exc is not None and is_auth_timeout_error(exc):
            detail = _prefer_exc_text(
                raw_reason,
                exc_text,
                ignored_reasons={SSH_AUTH_FAILED_REASON},
            )
            return "SSH_AUTH_TIMEOUT", "SSH authentication timed out", detail

        detail = _prefer_exc_text(
            raw_reason,
            exc_text,
            ignored_reasons=AUTH_FAILED_DETAIL_EXCEPTIONS,
        )
        return "AUTH_FAILED", "Authentication failed", detail

    if stage == k.STAGE_SSH or raw_reason == SSH_NEGOTIATION_FAILED_REASON:
        detail = _prefer_exc_text(
            raw_reason,
            exc_text,
            ignored_reasons={SSH_NEGOTIATION_FAILED_REASON},
        )
        return "SSH_NEGOTIATION_FAILED", SSH_NEGOTIATION_FAILED_REASON, detail

    if stage == k.STAGE_CONNECT:
        if _is_timeout_exception(exc):
            return (
                "SSH_CONNECT_TIMEOUT",
                "SSH/SFTP connection timed out",
                exc_text or raw_reason,
            )

        detail = _prefer_exc_text(
            raw_reason,
            exc_text,
            ignored_reasons={SFTP_INIT_FAILED_REASON},
        )
        return "SFTP_INIT_FAILED", SFTP_INIT_FAILED_REASON, detail

    if stage == k.STAGE_TRANSFER:
        if isinstance(exc, FileNotFoundError):
            return "FILE_NOT_FOUND", "File not found", exc_text or raw_reason

        if _is_timeout_exception(exc):
            return "TRANSFER_TIMEOUT", "File transfer timed out", exc_text or raw_reason

        if isinstance(exc, PermissionError):
            detail = _prefer_exc_text(
                raw_reason,
                exc_text,
                ignored_reasons={FILE_TRANSFER_FAILED_REASON},
            )
            return (
                "TRANSFER_PERMISSION_DENIED",
                "Permission denied during transfer",
                detail,
            )

        if isinstance(exc, paramiko.SSHException):
            detail = _prefer_exc_text(
                raw_reason,
                exc_text,
                ignored_reasons={FILE_TRANSFER_FAILED_REASON},
            )
            return "SSH_TRANSFER_FAILED", "SSH/SFTP transfer failed", detail

        if isinstance(exc, OSError):
            detail = _prefer_exc_text(
                raw_reason,
                exc_text,
                ignored_reasons={FILE_TRANSFER_FAILED_REASON},
            )
            return "TRANSFER_IO_ERROR", "Filesystem error during transfer", detail

        if raw_reason == FILE_TRANSFER_FAILED_REASON:
            return "FILE_TRANSFER_FAILED", raw_reason, exc_text or None

    if raw_reason == "Failed to lock HOST or FILE_TASK":
        return "TASK_LOCK_FAILED", raw_reason, exc_text or None

    if raw_reason in {"Failed to lock HOST or HOST_TASK", "Failed to lock task"}:
        canonical = "Failed to lock task"
        detail = exc_text or _detail_from_reason(raw_reason, canonical)
        return "TASK_LOCK_FAILED", canonical, detail

    if raw_reason == "Host not found in database":
        return "HOST_NOT_FOUND", raw_reason, None

    if raw_reason == "Post-transfer update failed":
        return "FINALIZE_UPDATE_FAILED", raw_reason, exc_text or None

    return None, None, None


def _classify_stage_reason(
    raw_reason: str,
    exc: Optional[Exception],
    exc_text: str,
    *,
    stage: Optional[str],
) -> ErrorTriple:
    """Classify stable stage-level worker failures."""
    _ = exc
    stage_rules = [
        (k.STAGE_DISCOVERY, DISCOVERY_FAILED_REASON, "DISCOVERY_FAILED"),
        (k.STAGE_BACKLOG, BACKLOG_PROMOTION_FAILED_REASON, "BACKLOG_PROMOTION_FAILED"),
        (
            k.STAGE_CONNECTIVITY,
            CONNECTIVITY_TEST_FAILED_REASON,
            "CONNECTIVITY_CHECK_FAILED",
        ),
        (k.STAGE_TRANSACTION, DB_TRANSACTION_FAILED_REASON, "DB_TRANSACTION_FAILED"),
        (k.STAGE_UPDATE_STATS, STATS_UPDATE_FAILED_REASON, "STATS_UPDATE_FAILED"),
        (k.STAGE_HOST_CREATE, HOST_CREATE_FAILED_REASON, "HOST_CREATE_FAILED"),
        (k.STAGE_QUEUE, HOST_TASK_QUEUE_FAILED_REASON, "HOST_TASK_QUEUE_FAILED"),
    ]

    for stage_name, canonical_reason, code in stage_rules:
        if stage == stage_name or raw_reason == canonical_reason:
            detail = _prefer_exc_text(
                raw_reason,
                exc_text,
                ignored_reasons={canonical_reason},
            )
            return code, canonical_reason, detail

    if stage == k.STAGE_READ and raw_reason == EMPTY_REQUEST_REASON:
        return "EMPTY_REQUEST", EMPTY_REQUEST_REASON, None

    if stage == k.STAGE_COMMAND and raw_reason == UNSUPPORTED_COMMAND_REASON:
        return "UNSUPPORTED_COMMAND", UNSUPPORTED_COMMAND_REASON, None

    if stage == k.STAGE_PARSE and raw_reason == INVALID_HOST_ID_REASON:
        return "INVALID_HOST_ID", INVALID_HOST_ID_REASON, None

    return None, None, None


def _canonicalize_error_reason(
    reason: Optional[str],
    exc: Optional[Exception],
    stage: Optional[str] = None,
) -> ErrorTriple:
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

    if not raw_reason:
        return _classify_empty_reason(exc, exc_text)

    classifiers = (
        _classify_validation_error,
        _classify_app_analise_error,
        _classify_site_error,
        _classify_backup_stage_error,
        _classify_stage_reason,
    )

    for classifier in classifiers:
        code, canonical_reason, detail = classifier(
            raw_reason,
            exc,
            exc_text,
            stage=stage,
        )
        if code is not None:
            return code, canonical_reason, detail

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


class AppAnaliseServiceResponseError(ExternalServiceTransientError):
    """
    Raised when appAnalise replies with an explicit service-side error.

    The request reached the remote processor and produced a deliberate error
    identifier or error payload. This is operationally different from a
    malformed RF.Fusion payload, so the worker should freeze the task.
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


class AppAnaliseInvalidSuccessPayloadError(Exception):
    """
    Raised when appAnalise answers without `Error` but with invalid success data.

    The service stayed reachable and replied, but the returned success payload
    is not usable by RF.Fusion. Operators should inspect the task manually.
    """
    pass


PROCESSING_FREEZE_DETAILS = {
    AppAnaliseServiceResponseError: "APP_ANALISE returned service error, task frozen for manual review",
    AppAnaliseReadTimeoutError: "APP_ANALISE read timeout, task frozen for manual review",
    AppAnaliseFileUnavailableError: "APP_ANALISE file unavailable, task frozen for manual review",
    AppAnaliseInvalidSuccessPayloadError: "APP_ANALISE returned invalid success payload, task frozen for manual review",
    ExternalServiceTransientError: "Transient appAnalise service failure, task frozen for manual review",
}

def is_definitive_appanalise_processing_error(exc: BaseException | None) -> bool:
    """Return whether one appAnalise response should be treated as final error."""
    definitive_details = (
        k.APP_ANALISE_NO_READABLE_FILES_IN_ZIP_DETAIL,
        k.APP_ANALISE_EMPTY_SPEC_DATA_DETAIL,
        k.APP_ANALISE_NO_SPECTRAL_DATA_DETAIL,
    )
    return (
        isinstance(exc, AppAnaliseServiceResponseError)
        and any(detail in str(exc) for detail in definitive_details)
    )


def should_freeze_processing_task(exc: BaseException | None) -> bool:
    """Return whether one processing failure should freeze the task."""
    if is_definitive_appanalise_processing_error(exc):
        # EmptySpecData means the artifact has no usable spectrum payload.
        return False
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
            builtins.TimeoutError,
            OperationTimeoutError,
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
        if _is_timeout_exception(exc)
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
        err.capture("Discovery failed", stage=k.STAGE_DISCOVERY, exc=e)
        err.log_error(host_id=..., task_id=...)

    The handler is intentionally simple:
        - capture the first meaningful failure
        - keep structured context next to it
        - let the caller decide later whether to log, persist, or both
    """

    def __init__(self, log: "ErrorEventLogger") -> None:
        self.logger = log
        self.reason: Optional[str] = None
        self.stage: Optional[str] = None
        self.exc: Optional[Exception] = None
        self.context: Dict[str, Any] = {}

    def _store_capture(
        self,
        reason: str,
        stage: Optional[str] = None,
        exc: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Store the first meaningful failure and ignore later noise."""
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
        stage: Optional[str] = None,
        exc: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """
        Register one workflow failure at the exception boundary.

        The first captured error wins. Later cleanup noise is ignored.
        """
        self._store_capture(
            reason=reason,
            stage=stage,
            exc=exc,
            **context,
        )

    def set(
        self,
        reason: str,
        stage: Optional[str] = None,
        exc: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Backward-compatible alias. Prefer `capture()` in new code."""
        self._store_capture(
            reason=reason,
            stage=stage,
            exc=exc,
            **context,
        )

    @property
    def triggered(self) -> bool:
        return self.reason is not None

    @property
    def msg(self) -> str:
        """Return a compact stage-prefixed message for quick human reads."""
        if self.stage:
            return f"{self.stage}: {self.reason}"
        return self.reason or ""

    def _merge_context(self, **runtime_context: Any) -> Dict[str, Any]:
        """Return stored and runtime context without `None` values."""
        merged_context = dict(self.context)
        for key, value in runtime_context.items():
            if value is not None:
                merged_context[str(key)] = value
        return merged_context

    def log_error(self, **runtime_context: Any) -> None:
        """
        Emit one structured error log enriched with stored and runtime context.

        `self.context` holds the facts captured at the failure point. The
        optional `runtime_context` lets callers attach outer-loop information
        only available at log time, such as traceback or aggregate counters.
        """
        merged_context = self._merge_context(**runtime_context)
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



class OperationTimeoutError(Exception):
    """
    Raised when a function exceeds the allowed timeout.

    This is the module-local timeout abstraction returned by `run_with_timeout`
    so callers do not need to know about `concurrent.futures`.
    """
    pass


# Backward-compatible public name used by tests and older imports.
TimeoutError = OperationTimeoutError


def run_with_timeout(func: Callable[[], Any], timeout: float) -> Any:
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
        raise OperationTimeoutError(
            f"Operation timed out after {timeout} seconds"
        )

    except Exception:
        raise
