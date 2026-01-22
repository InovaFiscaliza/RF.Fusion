from __future__ import annotations
import sys
import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from . import tools


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

# =====================================================================
# Filter class + parse_filter wrapper
# =====================================================================
class Filter:
    """Unified handler for parsing, validating, and applying file filters.

    Provides standardized logic for applying file-level filters based on
    metadata (name, modification date, extension, etc.) and supports multiple
    modes such as RANGE, FILE, LAST, ALL, and AGENT.

    Attributes:
        raw (Union[str, dict, None]): Raw filter configuration (JSON or dict).
        data (dict): Normalized filter configuration after parsing/validation.
        log (Optional[Any]): Optional logger for diagnostic messages.
    """

    # ------------------------------------------------------------------
    # Filter mode constants
    # ------------------------------------------------------------------
    MODE_NONE = "NONE"
    MODE_ALL = "ALL"
    MODE_FILE = "FILE"
    MODE_RANGE = "RANGE"
    MODE_LAST = "LAST"
    MODE_REDISCOVERY = "REDISCOVERY"  # reserved for future use

    VALID_MODES = (
        MODE_NONE,
        MODE_ALL,
        MODE_FILE,
        MODE_RANGE,
        MODE_LAST,
        MODE_REDISCOVERY,
    )

    def __init__(self, filter_raw: Union[str, Dict[str, Any], None] = None, log: Optional[Any] = None):
        """Initialize a Filter instance.

        Args:
            filter_raw: JSON string or dict containing filter configuration.
            log: Optional logger instance for diagnostic output.
        """
        self.log = log
        self.raw = filter_raw
        self.data = self._parse_and_validate()

    # ------------------------------------------------------------------
    # Parsing & Validation
    # ------------------------------------------------------------------
    @staticmethod
    def _default_dict() -> Dict[str, Any]:
        """Return a default filter dictionary.

        Returns:
            dict: Default filter configuration with neutral values.
        """
        return dict(
            mode=Filter.MODE_NONE,
            start_date=None,
            end_date=None,
            last_n_files=None,
            extension=None,
            file_name=None,
            file_path=k.DEFAULT_DATA_FOLDER,
            agent="local",
        )

    def _parse_and_validate(self) -> Dict[str, Any]:
        """Parse and validate the filter configuration.

        Returns:
            dict: Normalized and validated filter configuration.
        """
        try:
            f = self._parse_raw()
            self._validate(f)
            return f
        except Exception as e:
            if self.log:
                self.log.warning(f"[Filter] Parse/validate failed: {e}")
            return self._default_dict()

    def _parse_raw(self) -> Dict[str, Any]:
        """Normalize raw JSON or dict input into canonical structure.

        Returns:
            dict: Parsed configuration dictionary.
        """
        if not self.raw:
            return self._default_dict()

        if isinstance(self.raw, str):
            try:
                f = json.loads(self.raw)
            except Exception as e:
                if self.log:
                    self.log.entry(f"[Filter] JSON parse error: {e}")
                return self._default_dict()
        elif isinstance(self.raw, dict):
            f = dict(self.raw)
        else:
            return self._default_dict()

        return {
            "mode": str(f.get("mode", Filter.MODE_NONE)).upper().strip(),
            "start_date": f.get("start_date"),
            "end_date": f.get("end_date"),
            "last_n_files": f.get("last_n_files"),
            "extension": f.get("extension"),
            "file_path": f.get("file_path",k.DEFAULT_DATA_FOLDER),
            "file_name": f.get("file_name"),
            "agent": f.get("agent"),
        }

    def _validate(self, f: Dict[str, Any]) -> None:
        """
        Validate and normalize the parsed filter dictionary.

        This method ensures consistency among filter fields, enforcing mode-specific
        constraints and type normalization. Each supported mode activates only the
        fields relevant to its semantics, while the others are nullified.

        Behavior by mode:
            - RANGE: Validates date boundaries (`start_date`, `end_date`), ensuring
            correct chronological order and ISO 8601 formatting.
            - FILE: Forces `agent = "local"`, normalizes `file_name`, and disables
            `extension` when redundant. If `file_name` is empty, reverts mode to NONE.
            - LAST: Converts `last_n_files` to an integer ≥ 1; invalid entries are nulled.
            - ALL/NONE: Retain only `extension` and `agent`.

        Additionally:
            - `extension` is normalized to lowercase and prefixed with '.' if missing.
            - `agent` is coerced to 'local' or 'remote' (default = 'remote').
            - Unused fields are explicitly set to None to simplify downstream logic.

        Args:
            f (dict): Parsed filter configuration (may be user-provided or default).

        Returns:
            None. The input dictionary is modified in-place.
    """
        mode = f["mode"]

        # --- Normalize agent ---
        agent = f.get("agent")
        if isinstance(agent, str):
            agent = agent.strip().lower()
            if agent not in ("local", "remote"):
                agent = "remote"
        else:
            agent = "remote"

        # --- RANGE mode ---
        if mode == Filter.MODE_RANGE:
            start, end = self._safe_date(f.get("start_date")), self._safe_date(f.get("end_date"))
            if start and end and start > end:
                start, end = end, start
            f["start_date"], f["end_date"] = start, end

        # --- FILE mode ---
        elif mode == Filter.MODE_FILE:
            agent = "local"  # file mode sempre local
            file_name = (f.get("file_name") or "").strip().lower()
            if not file_name:
                f["mode"] = Filter.MODE_NONE
            elif "." in os.path.basename(file_name):
                f["extension"] = None
            f["file_name"] = file_name

        # --- LAST mode ---
        elif mode == Filter.MODE_LAST:
            try:
                f["last_n_files"] = max(1, int(f["last_n_files"]))
            except Exception:
                f["last_n_files"] = None
                

        # --- Extension normalization (all modes) ---
        ext = f.get("extension")
        if isinstance(ext, str):
            ext = ext.strip().lower()
            if ext and not ext.startswith("."):
                ext = f".{ext}"
            f["extension"] = ext
        else:
            f["extension"] = None

        # --- Update final agent value ---
        f["agent"] = agent

        # --- Define active fields ---
        active_fields = {
            Filter.MODE_RANGE:          {"start_date", "end_date", "extension", "agent", "file_path"},
            Filter.MODE_FILE:           {"file_name", "extension", "agent","file_path"},
            Filter.MODE_LAST:           {"last_n_files", "extension", "agent","file_path"},
            Filter.MODE_ALL:            {"extension", "agent","file_path"},
            Filter.MODE_NONE:           {"extension", "agent","file_path"},
            Filter.MODE_REDISCOVERY:    {"extension", "agent","file_path"},
        }

        # --- Nullify unused fields ---
        all_fields = {"start_date", "end_date", "last_n_files", "extension", "file_name", "agent","file_path"}
        keep = active_fields.get(f["mode"], set())
        for key in all_fields - keep:
            f[key] = None

    # ------------------------------------------------------------------
    # Pattern Builder
    # ------------------------------------------------------------------
    def _build_pattern(self) -> str:
        """
        Safely construct a file matching pattern for discovery operations.

        Resolves interactions between 'file_name' and 'extension' fields while
        preventing malformed expressions such as '*.bin.bin'.

        Rules:
            - If file_name is provided:
                - If file_name has an extension → use as-is.
                - If file_name has no extension and extension exists → append it.
                - Otherwise, use file_name unchanged.

            - If file_name is not provided:
                - FILE mode → return "*".
                - Other modes:
                    - If extension exists → "*.<ext>"
                    - Otherwise           → "*"

        Additional Behavior:
            - Ensures wildcard '*' prefix if missing.
            - Normalizes extension by adding a leading dot if needed.
            - Strips stray quotes.
        """

        file_name = self.data.get("file_name")
        extension = self.data.get("extension")
        mode      = (self.data.get("mode") or "").upper()

        # Normalize inputs
        if file_name:
            file_name = file_name.strip().replace('"', "").replace("'", "")
        if extension:
            extension = extension.strip().replace('"', "").replace("'", "")

        # --------------------------------------------------------------
        # FILE mode
        # --------------------------------------------------------------
        if mode == Filter.MODE_FILE:
            if not file_name:
                return "*"

            # ensure wildcard prefix
            if not any(file_name.startswith(p) for p in ("*", "?")):
                file_name = "*" + file_name

            base, ext_in_name = os.path.splitext(file_name)

            if ext_in_name:
                return file_name

            if extension:
                if not extension.startswith("."):
                    extension = "." + extension
                return file_name + extension

            return file_name

        # --------------------------------------------------------------
        # Other modes
        # --------------------------------------------------------------
        if file_name:
            if not any(file_name.startswith(p) for p in ("*", "?")):
                file_name = "*" + file_name

            base, ext_in_name = os.path.splitext(file_name)

            if ext_in_name:
                return file_name

            if extension:
                if not extension.startswith("."):
                    extension = "." + extension
                return file_name + extension

            return file_name

        # --------------------------------------------------------------
        # No file_name
        # --------------------------------------------------------------
        if extension:
            if not extension.startswith("."):
                extension = "." + extension
            return "*" + extension

        return "*"



    @staticmethod
    def _safe_date(val: Any) -> Optional[str]:
        """Convert value to ISO8601 date string if valid.

        Args:
            val: Value to convert.

        Returns:
            Optional[str]: ISO8601 formatted date string, or None if invalid.
        """
        try:
            return datetime.fromisoformat(str(val).replace("Z", "")).isoformat()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Evaluation logic (Strategy dispatch)
    # ------------------------------------------------------------------
    def evaluate_database(
        self,
        host_id: int,
        search_type: Optional[int] = None,
        search_status: Optional[Union[int, List[int]]] = None,
    ) -> Dict[str, Any]:
        """
        Build SQL filtering metadata for FILE_TASK updates based on Filter semantics.

        Contract:
            • This method is DB-driven only.
            • No filesystem data.
            • MODE_FILE is resolved via SQL LIKE using file_name patterns.
            • Safe for very large backlogs (Celplan-scale).

        Returns:
            {
                "where": { ... },
                "extra_sql": "ORDER BY ... LIMIT ...",
                "msg_prefix": "Backup Pending"
            }
        """

        mode = (self.data.get("mode") or "").upper()

        # ============================================================
        # Base WHERE (always FK_HOST)
        # ============================================================
        where: Dict[str, Any] = {"FK_HOST": host_id}

        # ------------------------------------------------------------
        # NU_TYPE
        # ------------------------------------------------------------
        if search_type is not None:
            where["NU_TYPE"] = search_type

        # ------------------------------------------------------------
        # NU_STATUS
        # ------------------------------------------------------------
        if search_status is not None:
            if isinstance(search_status, (list, tuple)):
                where["NU_STATUS__in"] = list(search_status)
            else:
                where["NU_STATUS"] = search_status

        # ------------------------------------------------------------
        # Extension filter (optional, orthogonal)
        # ------------------------------------------------------------
        extension = self.data.get("extension")
        if isinstance(extension, str):
            extension = extension.strip().lower() or None

        if extension:
            where["NA_EXTENSION__like"] = f"%{extension}"

        # ------------------------------------------------------------
        # Extra SQL
        # ------------------------------------------------------------
        extra_sql = ""

        # ------------------------------------------------------------
        # Message prefix
        # ------------------------------------------------------------
        msg_prefix = tools.compose_message(
            search_type, search_status, "", "", prefix_only=True
        )

        # ============================================================
        # MODE = ALL
        # ============================================================
        if mode == Filter.MODE_ALL:
            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix,
            }

        # ============================================================
        # MODE = NONE / REDISCOVERY
        # ============================================================
        if mode in (Filter.MODE_NONE, Filter.MODE_REDISCOVERY):
            return {
                "where": None,
                "extra_sql": "",
                "msg_prefix": None,
            }

        # ============================================================
        # MODE = FILE   (PATTERN-BASED)
        # ============================================================
        if mode == Filter.MODE_FILE:

            file_name = self.data.get("file_name")

            if not isinstance(file_name, str) or not file_name.strip():
                return {
                    "where": None,
                    "extra_sql": "",
                    "msg_prefix": None,
                }

            # Normalize wildcard: "*" → "%"
            sql_pattern = file_name.strip().replace("*", "%")

            where["NA_HOST_FILE_NAME__like"] = sql_pattern

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix,
            }

        # ============================================================
        # MODE = RANGE
        # ============================================================
        if mode == Filter.MODE_RANGE:

            start = self.data.get("start_date")
            end = self.data.get("end_date")

            if start and end:
                where["DT_FILE_CREATED__between"] = (start, end)
            elif start:
                where["DT_FILE_CREATED__gte"] = start
            elif end:
                where["DT_FILE_CREATED__lte"] = end
            else:
                return {
                    "where": None,
                    "extra_sql": "",
                    "msg_prefix": None,
                }

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix,
            }

        # ============================================================
        # MODE = LAST
        # ============================================================
        if mode == Filter.MODE_LAST:

            last_n = int(self.data.get("last_n_files", 0))
            if last_n <= 0:
                return {
                    "where": None,
                    "extra_sql": "",
                    "msg_prefix": None,
                }

            extra_sql = f"ORDER BY DT_FILE_CREATED DESC LIMIT {last_n}"

            return {
                "where": where,
                "extra_sql": extra_sql,
                "msg_prefix": msg_prefix,
            }

        # ============================================================
        # Defensive fallback
        # ============================================================
        return {
            "where": where,
            "extra_sql": extra_sql,
            "msg_prefix": msg_prefix,
        }


        
    def evaluate_metadata(self, metadata_list: list):
        """
        Apply secondary metadata-based filters on a list of FileMetadata objects.

        This function operates EXCLUSIVELY on FileMetadata instances.
        Legacy dict-based metadata is no longer supported by design.

        Safety protections enforced before any semantic filtering:
            1. Minimum file size (KB)
            2. Minimum file age (ignore files too recently created)

        Supported modes:
            FILE / ALL / NONE → extension-only filtering
            RANGE             → date interval filtering
            LAST              → last N files by creation timestamp
        """

        if not metadata_list:
            return []

        # ------------------------------------------------------------------
        # 1) Minimum file size protection
        # ------------------------------------------------------------------
        min_size_kb = getattr(k, "MIN_FILE_SIZE_KB", 0)

        filtered = [
            m for m in metadata_list
            if m.VL_FILE_SIZE_KB >= min_size_kb
        ]

        if not filtered:
            return []

        # ------------------------------------------------------------------
        # 2) Minimum file age protection
        # ------------------------------------------------------------------
        min_age_minutes = getattr(k, "MIN_FILE_AGE_MINUTES", 30)
        age_threshold = datetime.now() - timedelta(minutes=min_age_minutes)

        filtered = [
            m for m in filtered
            if m.DT_FILE_CREATED and m.DT_FILE_CREATED <= age_threshold
        ]

        if not filtered:
            return []

        metadata_list = filtered

        # ------------------------------------------------------------------
        # Extract filter parameters
        # ------------------------------------------------------------------
        mode = (self.data.get("mode") or "").upper()
        extension = (self.data.get("extension") or "").lower()

        # ==================================================================
        # FILE / ALL / NONE
        # ==================================================================
        if mode in ("FILE", "ALL", "NONE"):
            if extension:
                return [
                    m for m in metadata_list
                    if m.NA_EXTENSION.lower() == extension
                ]
            return metadata_list

        # ==================================================================
        # RANGE mode
        # ==================================================================
        if mode == "RANGE":

            start_raw = self.data.get("start_date")
            end_raw = self.data.get("end_date")

            start = None
            end = None

            # Parse start date
            try:
                if isinstance(start_raw, datetime):
                    start = start_raw
                elif isinstance(start_raw, str) and start_raw.strip():
                    start = datetime.fromisoformat(start_raw)
            except Exception:
                pass

            # Parse end date
            try:
                if isinstance(end_raw, datetime):
                    end = end_raw
                elif isinstance(end_raw, str) and end_raw.strip():
                    end = datetime.fromisoformat(end_raw)
            except Exception:
                pass

            filtered = []

            for m in metadata_list:
                ts = m.DT_FILE_CREATED
                if not ts:
                    continue

                if start and not end and ts >= start:
                    filtered.append(m)
                    continue

                if end and not start and ts <= end:
                    filtered.append(m)
                    continue

                if start and end and start <= ts <= end:
                    filtered.append(m)
                    continue

                if not start and not end:
                    filtered.append(m)

            if extension:
                filtered = [
                    m for m in filtered
                    if m.NA_EXTENSION.lower() == extension
                ]

            return filtered

        # ==================================================================
        # LAST mode
        # ==================================================================
        if mode == "LAST":
            last_n = int(self.data.get("last_n") or 0)

            ordered = sorted(
                metadata_list,
                key=lambda m: m.DT_FILE_CREATED or datetime.min
            )

            if extension:
                ordered = [
                    m for m in ordered
                    if m.NA_EXTENSION.lower() == extension
                ]

            return ordered[-last_n:] if last_n > 0 else ordered

        # ==================================================================
        # Unknown / fallback mode
        # ==================================================================
        if extension:
            return [
                m for m in metadata_list
                if m.NA_EXTENSION.lower() == extension
            ]

        return metadata_list
