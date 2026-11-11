"""
Focused tests for the incremental RFFUSION_SUMMARY refresh engine.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, ensure_app_paths, load_module_from_path


ensure_app_paths()

refresh_engine_module = load_module_from_path(
    "test_summary_refresh_engine_module",
    str(APP_ROOT / "summary_handler" / "refresh_engine.py"),
)

DirtyScope = refresh_engine_module.DirtyScope
SummaryRefreshEngine = refresh_engine_module.SummaryRefreshEngine
_cwsm_signature = refresh_engine_module._cwsm_signature
_normalize_key = refresh_engine_module._normalize_key


class FakeSummaryDb:
    """Exercise domain decisions through the real public persistence contract."""

    def __init__(self) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            None.
        """
        self.replaced = {}
        self.in_transaction = False
        self.commits = 0
        self.rollbacks = 0
        self.audit = []

    def begin_transaction(self) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            None.
        """
        self.in_transaction = True

    def commit(self) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            None.
        """
        self.commits += 1
        self.in_transaction = False

    def rollback(self) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            None.
        """
        self.rollbacks += 1
        self.in_transaction = False

    def refresh_log_record(self, name: str, **fields: object) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            name (object): Controlled value supplied by the scenario.
            **fields (object): Explicit entity scope options.
        Returns:
            None.
        """
        self.audit.append((name, fields))

    def host_equipment_list_sources(self) -> dict[str, list[dict[str, object]]]:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return {"hosts": [], "equipment": []}

    def host_equipment_replace(self, rows: list[dict[str, object]]) -> int:
        """Support the domain scenario through the public persistence contract.

        Args:
            rows (object): Controlled value supplied by the scenario.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.replaced["HOST_EQUIPMENT_LINK"] = rows
        return len(rows)

    def site_equipment_list_sources(self, **scope: object) -> list[dict[str, object]]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return []

    def site_equipment_replace(
        self, rows: list[dict[str, object]], **scope: object
    ) -> int:
        """Support the domain scenario through the public persistence contract.

        Args:
            rows (object): Controlled value supplied by the scenario.
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.replaced["SITE_EQUIPMENT_METRIC"] = rows
        return len(rows)

    def site_equipment_list_affected(self, **scope: object) -> list[int]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return sorted(scope["equipment_ids"])

    def host_visit_list_hosts(self, **scope: object) -> list[int]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return []

    def host_monthly_list_sources(self, **scope: object) -> list[dict[str, object]]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return []

    def host_monthly_replace(
        self, rows: list[dict[str, object]], **scope: object
    ) -> int:
        """Support the domain scenario through the public persistence contract.

        Args:
            rows (object): Controlled value supplied by the scenario.
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.replaced["HOST_MONTHLY_METRIC"] = rows
        return len(rows)

    def host_error_list_sources(self, **scope: object) -> list[dict[str, object]]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return []

    def host_error_replace(self, rows: list[dict[str, object]], **scope: object) -> int:
        """Support the domain scenario through the public persistence contract.

        Args:
            rows (object): Controlled value supplied by the scenario.
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.replaced["HOST_ERROR_METRIC"] = rows
        return len(rows)

    def host_metric_list_sources(self, **scope: object) -> list[dict[str, object]]:
        """Support the domain scenario through the public persistence contract.

        Args:
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        return []

    def host_metric_replace(
        self, rows: list[dict[str, object]], **scope: object
    ) -> int:
        """Support the domain scenario through the public persistence contract.

        Args:
            rows (object): Controlled value supplied by the scenario.
            **scope (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.replaced["HOST_METRIC"] = rows
        return len(rows)


class FakeSummaryLog:
    def __init__(self) -> None:
        """Support the domain scenario through the public persistence contract.

        Args:
            None.
        Returns:
            None.
        """
        self.events = []
        self.warning_events = []

    def event(self, event, **fields):
        """Support the domain scenario through the public persistence contract.

        Args:
            event (object): Controlled value supplied by the scenario.
            **fields (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.events.append((event, fields))

    def warning_event(self, event, **fields):
        """Support the domain scenario through the public persistence contract.

        Args:
            event (object): Controlled value supplied by the scenario.
            **fields (object): Explicit entity scope options.
        Returns:
            object: Deterministic fixture result consumed by the scenario.
        """
        self.warning_events.append((event, fields))


class SummaryWorkerEngineTests(unittest.TestCase):
    def test_location_timeline_preserves_returns_and_marks_both_overlaps(self) -> None:
        """Verify location timeline preserves returns and marks both overlaps.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        visits = [
            {
                "FK_SITE": 48,
                "DT_FIRST_SEEN_AT": datetime(2024, 1, 1),
                "DT_LAST_SEEN_AT": datetime(2024, 8, 1),
            },
            {
                "FK_SITE": 58,
                "DT_FIRST_SEEN_AT": datetime(2024, 8, 26),
                "DT_LAST_SEEN_AT": datetime(2024, 9, 3),
            },
            {
                "FK_SITE": 171,
                "DT_FIRST_SEEN_AT": datetime(2024, 9, 17),
                "DT_LAST_SEEN_AT": datetime(2024, 9, 24),
            },
            {
                "FK_SITE": 170,
                "DT_FIRST_SEEN_AT": datetime(2024, 9, 21),
                "DT_LAST_SEEN_AT": datetime(2024, 9, 23),
            },
            {
                "FK_SITE": 48,
                "DT_FIRST_SEEN_AT": datetime(2025, 1, 1),
                "DT_LAST_SEEN_AT": datetime(2026, 9, 14),
            },
        ]
        db.host_visit_list_hosts = lambda **kwargs: [10]
        db.host_visit_list_sources = lambda host_id: visits
        published = []
        db.host_visit_replace = lambda host_id, rows: published.extend(rows) or len(
            rows
        )
        engine = SummaryRefreshEngine(db=db, logger=FakeSummaryLog())
        count, _ = engine.refresh_location_timeline(host_ids={10})
        self.assertEqual(count, 5)
        self.assertEqual([row["FK_SITE"] for row in published], [48, 58, 171, 170, 48])
        self.assertEqual([row["IS_OVERLAPPING"] for row in published], [0, 0, 1, 1, 0])
        self.assertEqual(published[2]["DT_LAST_SEEN_AT"], datetime(2024, 9, 24))

    def test_location_timeline_clears_hosts_without_dated_spectra(self) -> None:
        """Verify location timeline clears hosts without dated spectra.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        db.host_visit_list_hosts = lambda **kwargs: [10]
        db.host_visit_list_sources = lambda host_id: []
        published = []
        db.host_visit_replace = (
            lambda host_id, rows: published.append((host_id, rows)) or 0
        )
        engine = SummaryRefreshEngine(db=db, logger=FakeSummaryLog())
        self.assertEqual(engine.refresh_location_timeline(host_ids={10})[0], 0)
        self.assertEqual(published, [(10, [])])

    def test_dirty_scope_merges_all_outbox_rows(self) -> None:
        """Verify dirty scope merges all outbox rows.

        Args:
            None.
        Returns:
            None.
        """
        scope = DirtyScope.from_outbox_rows(
            [
                {
                    "ID_OUTBOX": 1,
                    "NA_SCOPE_TYPE": "host",
                    "NA_SCOPE_VALUE": "10",
                },
                {
                    "ID_OUTBOX": 2,
                    "NA_SCOPE_TYPE": "host",
                    "NA_SCOPE_VALUE": "11",
                },
                {
                    "ID_OUTBOX": 3,
                    "NA_SCOPE_TYPE": "site",
                    "NA_SCOPE_VALUE": "100",
                },
                {
                    "ID_OUTBOX": 4,
                    "NA_SCOPE_TYPE": "reference_month",
                    "NA_SCOPE_VALUE": "2026-05-13",
                },
                {
                    "ID_OUTBOX": 5,
                    "NA_SCOPE_TYPE": "reference_month",
                    "NA_SCOPE_VALUE": "2026-05",
                },
                {
                    "ID_OUTBOX": 6,
                    "NA_SCOPE_TYPE": "host",
                    "NA_SCOPE_VALUE": "12",
                },
                {
                    "ID_OUTBOX": 7,
                    "NA_SCOPE_TYPE": "equipment",
                    "NA_SCOPE_VALUE": "200",
                },
                {
                    "ID_OUTBOX": 8,
                    "NA_SCOPE_TYPE": "equipment",
                    "NA_SCOPE_VALUE": "201",
                },
                {
                    "ID_OUTBOX": 9,
                    "NA_SCOPE_TYPE": "full_reconcile",
                    "NA_SCOPE_VALUE": "*",
                },
            ]
        )

        self.assertEqual(scope.host_ids, {10, 11, 12})
        self.assertEqual(scope.site_ids, {100})
        self.assertEqual(scope.equipment_ids, {200, 201})
        self.assertEqual(scope.reference_months, {"2026-05-01"})
        self.assertTrue(scope.full_reconcile)

    def test_dirty_scope_rejects_invalid_outbox_row(self) -> None:
        """Verify dirty scope rejects invalid outbox row.

        Args:
            None.
        Returns:
            None.
        """
        with self.assertRaisesRegex(RuntimeError, "Invalid SUMMARY_OUTBOX row"):
            DirtyScope.from_outbox_rows(
                [
                    {
                        "ID_OUTBOX": 99,
                        "NA_SCOPE_TYPE": "host",
                        "NA_SCOPE_VALUE": "not-an-int",
                    }
                ]
            )

    def test_host_equipment_link_prefers_exact_matches_over_prefixes(
        self,
    ) -> None:
        """Prefer an exact automatic match over a competing prefix candidate.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        datasets = {
            "FROM BPDATA.HOST": [
                {"FK_HOST": 1, "NA_HOST_NAME": "CWSM-22010007"},
                {"FK_HOST": 2, "NA_HOST_NAME": "rfeye002073"},
                {"FK_HOST": 3, "NA_HOST_NAME": "rfeye002073-extra"},
            ],
            "FROM RFDATA.DIM_SPECTRUM_EQUIPMENT": [
                {"FK_EQUIPMENT": 10, "NA_EQUIPMENT": "cwsm211007"},
                {"FK_EQUIPMENT": 20, "NA_EQUIPMENT": "rfeye002073"},
            ],
        }

        db.host_equipment_list_sources = lambda: {
            "hosts": [
                {"ID_HOST": row["FK_HOST"], "NA_HOST_NAME": row["NA_HOST_NAME"]}
                for row in datasets["FROM BPDATA.HOST"]
            ],
            "equipment": [
                {
                    "ID_EQUIPMENT": row["FK_EQUIPMENT"],
                    "NA_EQUIPMENT": row["NA_EQUIPMENT"],
                }
                for row in datasets["FROM RFDATA.DIM_SPECTRUM_EQUIPMENT"]
            ],
        }
        row_count, watermark = engine._refresh_host_equipment_link()

        rows = db.replaced["HOST_EQUIPMENT_LINK"]
        self.assertEqual(row_count, 3)
        self.assertIn("hosts=3", watermark)
        by_equipment = {
            row["FK_EQUIPMENT"]: row for row in rows if row["IS_PRIMARY_LINK"] == 1
        }

        self.assertEqual(by_equipment[10]["FK_HOST"], 1)
        self.assertEqual(by_equipment[10]["NA_MATCH_TYPE"], "cwsm_signature")
        self.assertNotIn("NA_HOST_NAME_NORMALIZED", by_equipment[10])
        self.assertEqual(by_equipment[20]["FK_HOST"], 2)
        self.assertEqual(by_equipment[20]["NA_MATCH_TYPE"], "exact_normalized")
        prefix = next(row for row in rows if row["FK_HOST"] == 3)
        self.assertEqual(prefix["NA_MATCH_TYPE"], "prefix_match")
        self.assertEqual(prefix["IS_PRIMARY_LINK"], 0)

    def test_host_equipment_link_rebuilds_catalog_matches_without_resolving_ties(self) -> None:
        """Recompute primary links after catalog changes and retain ambiguous pairs.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        engine = SummaryRefreshEngine(db=db, logger=FakeSummaryLog())
        sources = {
            "hosts": [
                {"ID_HOST": 1, "NA_HOST_NAME": "receiver-001"},
                {"ID_HOST": 2, "NA_HOST_NAME": "receiver-001"},
            ],
            "equipment": [{"ID_EQUIPMENT": 7, "NA_EQUIPMENT": "receiver-001"}],
        }
        db.host_equipment_list_sources = lambda: sources

        engine._refresh_host_equipment_link()
        rows = db.replaced["HOST_EQUIPMENT_LINK"]
        self.assertEqual({row["FK_HOST"] for row in rows}, {1, 2})
        self.assertTrue(all(row["IS_PRIMARY_LINK"] == 0 for row in rows))

        sources["hosts"][1]["NA_HOST_NAME"] = "other-receiver"
        engine._refresh_host_equipment_link()
        rows = db.replaced["HOST_EQUIPMENT_LINK"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["FK_HOST"], 1)
        self.assertEqual(rows[0]["IS_PRIMARY_LINK"], 1)

        sources["hosts"] = []
        engine._refresh_host_equipment_link()
        self.assertEqual(db.replaced["HOST_EQUIPMENT_LINK"], [])

    def test_host_equipment_link_matches_cwsm_family_by_station_suffix(self) -> None:
        """Verify host equipment link matches cwsm family by station suffix.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        datasets = {
            "FROM BPDATA.HOST": [
                {"FK_HOST": 1, "NA_HOST_NAME": "CWSM212031"},
                {"FK_HOST": 2, "NA_HOST_NAME": "CWSM220044"},
            ],
            "FROM RFDATA.DIM_SPECTRUM_EQUIPMENT": [
                {"FK_EQUIPMENT": 118, "NA_EQUIPMENT": "cwsm21100031"},
                {"FK_EQUIPMENT": 138, "NA_EQUIPMENT": "cwsm21100044"},
            ],
        }

        db.host_equipment_list_sources = lambda: {
            "hosts": [
                {"ID_HOST": row["FK_HOST"], "NA_HOST_NAME": row["NA_HOST_NAME"]}
                for row in datasets["FROM BPDATA.HOST"]
            ],
            "equipment": [
                {
                    "ID_EQUIPMENT": row["FK_EQUIPMENT"],
                    "NA_EQUIPMENT": row["NA_EQUIPMENT"],
                }
                for row in datasets["FROM RFDATA.DIM_SPECTRUM_EQUIPMENT"]
            ],
        }
        row_count, watermark = engine._refresh_host_equipment_link()

        rows = db.replaced["HOST_EQUIPMENT_LINK"]
        self.assertEqual(row_count, 2)
        self.assertIn("hosts=2", watermark)
        by_equipment = {
            row["FK_EQUIPMENT"]: row for row in rows if row["IS_PRIMARY_LINK"] == 1
        }

        self.assertEqual(by_equipment[118]["FK_HOST"], 1)
        self.assertEqual(by_equipment[118]["NA_MATCH_TYPE"], "cwsm_signature")
        self.assertEqual(by_equipment[138]["FK_HOST"], 2)
        self.assertEqual(by_equipment[138]["NA_MATCH_TYPE"], "cwsm_signature")

    def test_normalization_and_cwsm_signature_follow_sql_contract(self) -> None:
        """Verify normalization and cwsm signature follow sql contract.

        Args:
            None.
        Returns:
            None.
        """
        self.assertEqual(_normalize_key(" CWSM-22010007 "), "cwsm22010007")
        self.assertEqual(_cwsm_signature("cwsm22010007"), "cwsm07")
        self.assertEqual(_cwsm_signature("cwsm21100007"), "cwsm07")
        self.assertEqual(_cwsm_signature("cwsm211007"), "cwsm07")
        self.assertEqual(_cwsm_signature("cwsm21100031"), "cwsm31")
        self.assertEqual(_cwsm_signature("cwsm212031"), "cwsm31")
        self.assertEqual(_cwsm_signature("cwsm2110021"), "cwsm21")
        self.assertIsNone(_cwsm_signature("cwsm2110000"))
        self.assertIsNone(_cwsm_signature("rfeye002073"))

    def test_host_error_summary_keeps_latest_event_without_audit_bloat(self) -> None:
        """Verify host error summary keeps latest event without audit bloat.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        db.host_error_list_sources = lambda host_ids=None: [
            {
                "FK_HOST": 10,
                "NA_ERROR_SCOPE": "PROCESSING",
                "NA_TASK_STATE": "ERROR",
                "NA_ERROR_CODE": "bad_header",
                "NA_ERROR_SUMMARY": "Header error",
                "NA_RAW_MESSAGE": "Header error",
                "DT_EVENT_AT": datetime(2026, 5, 20, 10, 0, 0),
                "ID_SOURCE_ROW": 101,
            },
            {
                "FK_HOST": 10,
                "NA_ERROR_SCOPE": "PROCESSING",
                "NA_TASK_STATE": "ERROR",
                "NA_ERROR_CODE": "bad_header",
                "NA_ERROR_SUMMARY": "Header error",
                "NA_RAW_MESSAGE": "Header error",
                "DT_EVENT_AT": datetime(2026, 5, 20, 12, 30, 0),
                "ID_SOURCE_ROW": 202,
            },
        ]

        row_count, watermark = engine._refresh_host_error_summary()

        self.assertEqual(row_count, 1)
        self.assertEqual(watermark, "rows=1")
        row = db.replaced["HOST_ERROR_METRIC"][0]
        self.assertEqual(row["NU_ERROR_COUNT"], 2)
        self.assertEqual(row["NA_TASK_STATE"], "ERROR")
        self.assertEqual(row["DT_LAST_SEEN_AT"], datetime(2026, 5, 20, 12, 30, 0))
        self.assertEqual(row["ID_LAST_SOURCE_ROW"], 202)
        self.assertNotIn("NA_HOST_NAME", row)
        self.assertNotIn("NA_LAST_SOURCE_TABLE", row)
        self.assertNotIn("DT_REFRESHED_AT", row)

    def test_host_error_summary_collapses_known_code_detail_variants(self) -> None:
        """Verify host error summary collapses known code detail variants.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        db.host_error_list_sources = lambda host_ids=None: [
            {
                "FK_HOST": 10,
                "NA_ERROR_SCOPE": "PROCESSING",
                "NA_TASK_STATE": "ERROR",
                "NA_ERROR_CODE": "APP_ANALISE_FILE_UNAVAILABLE",
                "NA_ERROR_SUMMARY": (
                    "APP_ANALISE source file unavailable before request _ _4_DONE.zip]"
                ),
                "NA_RAW_MESSAGE": "APP_ANALISE source file unavailable before request _ _4_DONE.zip]",
                "DT_EVENT_AT": datetime(2026, 5, 20, 10, 0, 0),
                "ID_SOURCE_ROW": 101,
            },
            {
                "FK_HOST": 10,
                "NA_ERROR_SCOPE": "PROCESSING",
                "NA_TASK_STATE": "ERROR",
                "NA_ERROR_CODE": "APP_ANALISE_FILE_UNAVAILABLE",
                "NA_ERROR_SUMMARY": "APP_ANALISE file unavailable during processing",
                "NA_RAW_MESSAGE": "APP_ANALISE file unavailable during processing",
                "DT_EVENT_AT": datetime(2026, 5, 20, 12, 30, 0),
                "ID_SOURCE_ROW": 202,
            },
        ]

        row_count, _watermark = engine._refresh_host_error_summary()

        self.assertEqual(row_count, 1)
        row = db.replaced["HOST_ERROR_METRIC"][0]
        self.assertEqual(row["NA_ERROR_SUMMARY"], "APP_ANALISE file unavailable")
        self.assertEqual(row["NU_ERROR_COUNT"], 2)

    def test_full_reconcile_publishes_only_canonical_aggregates(self) -> None:
        """Verify full reconcile publishes only canonical aggregates.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        engine = SummaryRefreshEngine(db, FakeSummaryLog())
        refreshed = engine.refresh_all(reason="test")
        self.assertEqual(
            refreshed,
            [
                "HOST_EQUIPMENT_LINK",
                "SITE_EQUIPMENT_METRIC",
                "HOST_LOCATION_VISIT",
                "HOST_MONTHLY_METRIC",
                "HOST_ERROR_METRIC",
                "HOST_METRIC",
            ],
        )
        self.assertEqual(db.commits, 1)
        self.assertEqual(db.rollbacks, 0)
        self.assertFalse(
            any(name.startswith("WEBFUSION_USAGE") for name in db.replaced)
        )

    def test_host_events_avoid_spectra_and_global_matching(self) -> None:
        """Verify host events avoid spectra and global matching.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        engine = SummaryRefreshEngine(db, FakeSummaryLog())
        refreshed = engine.refresh_for_events(
            [{"NA_SCOPE_TYPE": "host", "NA_SCOPE_VALUE": "10"}]
        )
        self.assertEqual(
            set(refreshed), {"HOST_MONTHLY_METRIC", "HOST_METRIC", "HOST_ERROR_METRIC"}
        )
        self.assertNotIn("HOST_EQUIPMENT_LINK", db.replaced)
        self.assertNotIn("SITE_EQUIPMENT_METRIC", db.replaced)

    def test_empty_events_do_no_writes(self) -> None:
        """Verify empty events do no writes.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()
        self.assertEqual(
            SummaryRefreshEngine(db, FakeSummaryLog()).refresh_for_events([]), []
        )
        self.assertEqual(db.replaced, {})
        self.assertFalse(db.in_transaction)

    def test_rebuild_failure_rolls_back_and_records_sanitized_audit(self) -> None:
        """Verify rebuild failure rolls back and records sanitized audit.

        Args:
            None.
        Returns:
            None.
        """
        db = FakeSummaryDb()

        def fail(**scope: object) -> list[dict[str, object]]:
            """Support the domain scenario through the public persistence contract.

            Args:
                **scope (object): Explicit entity scope options.
            Returns:
                object: Deterministic fixture result consumed by the scenario.
            """
            raise RuntimeError("private")

        db.site_equipment_list_sources = fail
        with self.assertRaises(RuntimeError):
            SummaryRefreshEngine(db, FakeSummaryLog()).refresh_all(reason="test")
        self.assertEqual(db.rollbacks, 1)
        self.assertEqual(db.commits, 0)
        self.assertFalse(db.audit[-1][1]["success"])
        self.assertEqual(db.audit[-1][1]["error_message"], "RuntimeError")
