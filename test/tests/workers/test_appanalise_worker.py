"""
Validation tests for `appCataloga_file_bin_process_appAnalise.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appanalise_worker.py -q

What is covered here:
    - export decision rules for different hostnames
    - file metadata resolution for history and output files
    - filesystem helpers used to move original and resolved artifacts
    - retry message generation for transient appAnalise failures
"""

from __future__ import annotations

import json
import errno
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APPANALISE_ROOT,
    APP_ROOT,
    DB_ROOT,
    bind_real_package,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        with bind_real_package("appAnalise", APPANALISE_ROOT):
            worker = load_module_from_path(
                "test_appanalise_worker_module",
                str(APP_ROOT / "appCataloga_file_bin_process_appAnalise.py"),
            )
            processing = worker.processing_bin


class FakeWorkerLog:
    """Record worker log events so tests can assert meaningful side effects."""

    def __init__(self) -> None:
        self.entries = []
        self.errors = []
        self.warnings = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def event(self, event: str, **fields) -> None:
        self.entries.append((event, fields))

    def error_event(self, event: str, **fields) -> None:
        self.errors.append((event, fields))

    def warning_event(self, event: str, **fields) -> None:
        parts = [f"event={event}"]
        parts.extend(f"{k}={v}" for k, v in fields.items() if v is not None)
        self.warnings.append(" ".join(parts))

    def service_start(self, service: str) -> None:
        self.entries.append(("service_start", service))

    def service_stop(self, service: str) -> None:
        self.entries.append(("service_stop", service))

    def task_phase(self, service: str, **fields) -> None:
        self.entries.append(("task_phase", {"service": service, **fields}))

    def task_done(self, service: str, **fields) -> None:
        self.entries.append(("task_done", {"service": service, **fields}))

    def task_error(self, service: str, **fields) -> None:
        self.errors.append(("task_error", {"service": service, **fields}))

    def task_frozen(self, service: str, **fields) -> None:
        self.entries.append(("task_frozen", {"service": service, **fields}))


class FakeDbBkp:
    """Minimal FILE_TASK persistence double with in-memory call recording."""

    def __init__(self) -> None:
        self.in_transaction = False
        self.task_updates = []
        self.task_deletes = []
        self.history_updates = []
        self.statistics_updates = []
        self.transaction_events = []
        self.history_rows_affected = 1
        self.delete_rows_affected = 1

    def begin_transaction(self) -> None:
        self.in_transaction = True
        self.transaction_events.append("begin")

    def commit(self) -> None:
        self.transaction_events.append("commit")
        self.in_transaction = False

    def rollback(self) -> None:
        self.transaction_events.append("rollback")
        self.in_transaction = False

    def file_task_update(self, **kwargs):
        self.task_updates.append(kwargs)
        return {
            "success": True,
            "rows_affected": 1,
            "updated_fields": kwargs,
        }

    def file_task_delete(self, **kwargs):
        self.task_deletes.append(kwargs)
        return self.delete_rows_affected

    def file_history_update(self, **kwargs):
        self.history_updates.append(kwargs)
        return {
            "success": True,
            "rows_affected": self.history_rows_affected,
            "updated_fields": kwargs,
        }

    def host_task_statistics_create(self, **kwargs) -> None:
        self.statistics_updates.append(kwargs)


class FakeDbRfm:
    """Minimal RFDATA double for server-file registration on success."""

    def __init__(self) -> None:
        self.insert_file_calls = []
        self.bridge_calls = []

    def build_path(self, site_id: int) -> str:
        return f"site_{site_id}/catalog"

    def insert_file(self, **kwargs) -> int:
        self.insert_file_calls.append(kwargs)
        return 900 + len(self.insert_file_calls)

    def insert_bridge_spectrum_file(self, spectrum_ids, file_ids) -> None:
        self.bridge_calls.append((list(spectrum_ids), list(file_ids)))


class FakeDbRfmIngest:
    """RFDATA double for per-spectrum site resolution and spectrum insertion."""

    def __init__(self, *, site_id=501) -> None:
        self.site_id = site_id
        self.site_geography = {"FK_DISTRICT": 1}
        self.get_site_id_calls = []
        self.get_site_geography_calls = []
        self.refresh_site_geography_calls = []
        self.update_site_calls = []
        self.insert_site_calls = []
        self.insert_file_calls = []
        self.bridge_calls = []
        self.insert_spectrum_calls = []
        self.equipment_calls = []
        self.procedure_calls = []
        self.detector_calls = []
        self.trace_type_calls = []
        self.measure_unit_calls = []

    def get_site_id(self, data):
        self.get_site_id_calls.append(dict(data))
        return self.site_id

    def get_site_geography(self, site_id):
        self.get_site_geography_calls.append(site_id)
        return dict(self.site_geography)

    def refresh_site_geography(self, site_id, data, *, force_create_district=False):
        self.refresh_site_geography_calls.append(
            {
                "site_id": site_id,
                "data": dict(data),
                "force_create_district": force_create_district,
            }
        )
        return {
            "action": "updated",
            "site_id": site_id,
            "fk_district": 1000,
            "site_name": data.get("district"),
            "district_name": data.get("district"),
            "would_create_district": False,
        }

    def update_site(self, **kwargs):
        self.update_site_calls.append(kwargs)

    def insert_site(self, data, *, force_create_district=False):
        self.insert_site_calls.append(
            {
                "data": dict(data),
                "force_create_district": force_create_district,
            }
        )
        return self.site_id

    def insert_file(self, **kwargs):
        self.insert_file_calls.append(kwargs)
        return 900 + len(self.insert_file_calls)

    def insert_bridge_spectrum_file(self, spectrum_ids, file_ids):
        self.bridge_calls.append((list(spectrum_ids), list(file_ids)))

    def insert_procedure(self, procedure_name):
        self.procedure_calls.append(procedure_name)
        return 41

    def get_or_create_spectrum_equipment(self, equipment_name, *, equipment_type_hint=None):
        self.equipment_calls.append(
            {
                "name": equipment_name,
                "type_hint": equipment_type_hint,
            }
        )
        return len(self.equipment_calls)

    def insert_detector_type(self, detector_name):
        self.detector_calls.append(detector_name)
        return 51

    def insert_trace_type(self, trace_name):
        self.trace_type_calls.append(trace_name)
        return 61

    def insert_measure_unit(self, unit_name):
        self.measure_unit_calls.append(unit_name)
        return 71

    def insert_spectrum(self, data):
        self.insert_spectrum_calls.append(dict(data))
        return 800 + len(self.insert_spectrum_calls)


class FakeErr:
    """Small error double matching the worker's finalization contract."""

    def __init__(self, message: str = "", triggered: bool = False) -> None:
        self._message = message
        self.triggered = triggered
        self.stage = "PROCESS"

    def format_error(self) -> str:
        return self._message

    def format_persisted_error(self) -> str:
        return self._message


def build_test_file_meta(path: Path) -> dict:
    """Build worker-style file metadata for a real temporary file."""
    stat = path.stat()
    timestamp = datetime.fromtimestamp(stat.st_mtime)
    return {
        "file_path": str(path.parent),
        "file_name": path.name,
        "extension": path.suffix,
        "size_kb": max(1, int(stat.st_size / 1024) or 1),
        "dt_created": timestamp,
        "dt_modified": timestamp,
        "full_path": str(path),
    }


class ShouldExportTests(unittest.TestCase):
    """Validate host-family rules that decide export and source-file mapping."""

    def test_should_export_disables_mat_for_rfeye_hosts(self) -> None:
        self.assertFalse(processing.should_export("rfeye001234"))

    def test_should_export_enables_mat_for_cw_hosts(self) -> None:
        self.assertTrue(processing.should_export("CWSM21100001"))

    def test_should_export_disables_mat_for_ermx_hosts(self) -> None:
        self.assertFalse(processing.should_export("ERMxES03"))

    def test_should_export_disables_mat_for_unknown_hosts(self) -> None:
        self.assertFalse(processing.should_export("unknown_station"))


class SiteResolutionTests(unittest.TestCase):
    """Validate per-spectrum SITE resolution and selective discard behavior."""

    def test_upsert_site_refreshes_existing_site_when_district_is_missing(self) -> None:
        db = FakeDbRfmIngest(site_id=77)
        db.site_geography = {"FK_DISTRICT": None}
        fixed_site = {
            "longitude": -46.633308,
            "latitude": -23.55052,
            "altitude": 760.0,
            "longitude_raw": [-46.633308],
            "latitude_raw": [-23.55052],
            "altitude_raw": [760.0],
            "nu_gnss_measurements": 1,
            "geographic_path": None,
        }
        enriched_site = {
            **fixed_site,
            "state": "São Paulo",
            "county": "São Paulo",
            "district": "Campo Belo",
            "district_candidates": ["Campo Belo"],
        }

        with patch.object(
            processing.geolocation_utils,
            "reverse_geocode_site_data",
            return_value=enriched_site,
        ):
            site_id = processing.upsert_site(db, dict(fixed_site))

        self.assertEqual(site_id, 77)
        self.assertEqual(db.get_site_geography_calls, [77])
        self.assertEqual(len(db.refresh_site_geography_calls), 1)
        self.assertTrue(
            db.refresh_site_geography_calls[0]["force_create_district"]
        )
        self.assertEqual(len(db.update_site_calls), 1)

    def test_upsert_site_inserts_new_site_with_forced_district_creation(self) -> None:
        db = FakeDbRfmIngest(site_id=812)
        db.get_site_id = lambda data: False
        fixed_site = {
            "longitude": -46.633308,
            "latitude": -23.55052,
            "altitude": 760.0,
            "longitude_raw": [-46.633308],
            "latitude_raw": [-23.55052],
            "altitude_raw": [760.0],
            "nu_gnss_measurements": 1,
            "geographic_path": None,
        }
        enriched_site = {
            **fixed_site,
            "state": "São Paulo",
            "county": "São Paulo",
            "district": "Campo Belo",
            "district_candidates": ["Campo Belo"],
        }

        with patch.object(
            processing.geolocation_utils,
            "reverse_geocode_site_data",
            return_value=enriched_site,
        ):
            site_id = processing.upsert_site(db, dict(fixed_site))

        self.assertEqual(site_id, 812)
        self.assertEqual(len(db.insert_site_calls), 1)
        self.assertTrue(db.insert_site_calls[0]["force_create_district"])
        self.assertEqual(
            db.insert_site_calls[0]["data"]["district"],
            "Campo Belo",
        )

    def test_resolve_spectrum_sites_reuses_fixed_site_once(self) -> None:
        db = FakeDbRfmIngest(site_id=77)
        fixed_site = {
            "longitude": -36.543807,
            "latitude": -10.286181,
            "altitude": 10.0,
            "longitude_raw": [-36.543807],
            "latitude_raw": [-10.286181],
            "altitude_raw": [10.0],
            "nu_gnss_measurements": 1,
            "geographic_path": None,
        }
        bin_data = {
            "spectrum": [
                SimpleNamespace(site_data=dict(fixed_site)),
                SimpleNamespace(site_data=dict(fixed_site)),
            ]
        }

        site_ids = processing.resolve_spectrum_sites(db, bin_data)

        self.assertEqual(site_ids, [77, 77])
        self.assertEqual(len(db.get_site_id_calls), 1)
        self.assertEqual(len(db.update_site_calls), 1)
        self.assertEqual(bin_data["spectrum"][0].site_id, 77)
        self.assertEqual(bin_data["spectrum"][1].site_id, 77)

    def test_resolve_spectrum_sites_keeps_mobile_geometry_stable(self) -> None:
        db = FakeDbRfmIngest(site_id=91)
        mobile_site = {
            "longitude": -35.897411,
            "latitude": -7.230131,
            "altitude": 12.0,
            "longitude_raw": [-35.897411],
            "latitude_raw": [-7.230131],
            "altitude_raw": [12.0],
            "nu_gnss_measurements": 1,
            "geographic_path": "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",
        }
        bin_data = {
            "spectrum": [SimpleNamespace(site_data=dict(mobile_site))]
        }

        site_ids = processing.resolve_spectrum_sites(db, bin_data)

        self.assertEqual(site_ids, [91])
        self.assertEqual(len(db.get_site_id_calls), 1)
        self.assertEqual(len(db.update_site_calls), 0)

    def test_resolve_spectrum_sites_discards_only_bad_site_resolution(self) -> None:
        good_site = {
            "longitude": -36.543807,
            "latitude": -10.286181,
            "altitude": 10.0,
            "longitude_raw": [-36.543807],
            "latitude_raw": [-10.286181],
            "altitude_raw": [10.0],
            "nu_gnss_measurements": 1,
            "geographic_path": None,
        }
        bad_site = {
            "longitude": -35.897411,
            "latitude": -7.230131,
            "altitude": 12.0,
            "longitude_raw": [-35.897411],
            "latitude_raw": [-7.230131],
            "altitude_raw": [12.0],
            "nu_gnss_measurements": 1,
            "geographic_path": "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",
        }
        good_spectrum = SimpleNamespace(description="good", site_data=dict(good_site))
        bad_spectrum = SimpleNamespace(description="bad", site_data=dict(bad_site))
        bin_data = {
            "spectrum": [bad_spectrum, good_spectrum],
            "discarded_spectrum_count": 1,
        }
        fake_log = FakeWorkerLog()

        def fake_upsert_site(db_rfm, site_data):
            if site_data["latitude"] == bad_site["latitude"]:
                raise ValueError("State 'Unknown' not found in DIM_SITE_STATE")
            return 77

        with patch.object(processing, "upsert_site", side_effect=fake_upsert_site):
            site_ids = processing.resolve_spectrum_sites(
                db_rfm=object(),
                bin_data=bin_data,
                logger=fake_log,
            )

        self.assertEqual(site_ids, [77])
        self.assertEqual(len(bin_data["spectrum"]), 1)
        self.assertEqual(bin_data["spectrum"][0].description, "good")
        self.assertEqual(bin_data["discarded_spectrum_count"], 2)
        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_site_resolution_discard", fake_log.warnings[0])

    def test_resolve_spectrum_sites_keeps_infrastructure_failure_fatal(self) -> None:
        fixed_site = {
            "longitude": -36.543807,
            "latitude": -10.286181,
            "altitude": 10.0,
            "longitude_raw": [-36.543807],
            "latitude_raw": [-10.286181],
            "altitude_raw": [10.0],
            "nu_gnss_measurements": 1,
            "geographic_path": None,
        }
        bin_data = {
            "spectrum": [SimpleNamespace(site_data=dict(fixed_site))]
        }

        with patch.object(
            processing,
            "upsert_site",
            side_effect=RuntimeError("database connection lost"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                processing.resolve_spectrum_sites(
                    db_rfm=object(),
                    bin_data=bin_data,
                )

        self.assertIn("database connection lost", str(ctx.exception))


class SpectrumInsertTests(unittest.TestCase):
    """Validate FACT_SPECTRUM insertion side effects and metadata payloads."""

    def _build_spectrum(self, *, site_id=10, equipment_name="rfeye002106"):
        return SimpleNamespace(
            site_id=site_id,
            equipment_name=equipment_name,
            start_mega=70.0,
            stop_mega=80.0,
            ndata=1024,
            trace_length=1691,
            level_unit="dBm",
            processing="peak",
            start_dateidx=datetime(2026, 1, 31, 20, 18, 51),
            stop_dateidx=datetime(2026, 1, 31, 20, 19, 51),
            bw=18457,
            description="PMEC faixa 1",
            metadata={},
        )

    def test_insert_spectra_batch_always_registers_host_file_lineage(self) -> None:
        db = FakeDbRfmIngest()
        bin_data = {
            "method": "Drive test",
            "spectrum": [
                self._build_spectrum(site_id=10, equipment_name="keysight a"),
                self._build_spectrum(site_id=11, equipment_name="keysight b"),
            ],
        }

        spectrum_ids = processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="keysight_mobile",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(len(db.insert_file_calls), 1)
        self.assertEqual(len(db.bridge_calls), 1)
        self.assertEqual(db.bridge_calls[0][1], [901])
        self.assertEqual(len(spectrum_ids), 2)
        self.assertEqual(db.insert_spectrum_calls[0]["id_site"], 10)
        self.assertEqual(db.insert_spectrum_calls[1]["id_site"], 11)

    def test_insert_spectra_batch_keeps_host_file_for_allowlisted_family(self) -> None:
        db = FakeDbRfmIngest()
        bin_data = {
            "method": "Fixed logger",
            "spectrum": [self._build_spectrum(site_id=10)],
        }

        processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="rfeye002106",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(len(db.insert_file_calls), 1)
        self.assertEqual(len(db.bridge_calls), 1)
        self.assertEqual(db.bridge_calls[0][1], [901])

    def test_insert_spectra_batch_persists_antenna_and_others_in_js_metadata(self) -> None:
        db = FakeDbRfmIngest()
        spectrum = self._build_spectrum(site_id=10, equipment_name="keysight a")
        spectrum.metadata = {
            "antenna": {"Name": "RFE-ANT-01", "Height": "15m"},
            "others": {"gpsType": "Built-in", "attMode": "Auto"},
        }
        bin_data = {
            "method": "Drive test",
            "spectrum": [spectrum],
        }

        processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="keysight_mobile",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        js_metadata = json.loads(db.insert_spectrum_calls[0]["js_metadata"])
        self.assertEqual(js_metadata["antenna"]["Name"], "RFE-ANT-01")
        self.assertEqual(js_metadata["others"]["gpsType"], "Built-in")
        self.assertNotIn("discarded_spectrum_count", js_metadata)

    def test_insert_spectra_batch_collapses_partial_rfeye_growth_same_payload(self) -> None:
        db = FakeDbRfmIngest()
        spectrum_a = self._build_spectrum(site_id=10, equipment_name="rfeye002083")
        spectrum_b = self._build_spectrum(site_id=10, equipment_name="rfeye002083")
        spectrum_a.description = "PMRD (Faixa 4 de 4)."
        spectrum_b.description = "PMRD (Faixa 4 de 4)."
        spectrum_a.start_mega = spectrum_b.start_mega = 470.0
        spectrum_a.stop_mega = spectrum_b.stop_mega = 700.0
        spectrum_a.ndata = spectrum_b.ndata = 5888
        spectrum_a.trace_length = 168
        spectrum_b.trace_length = 338
        spectrum_a.start_dateidx = spectrum_b.start_dateidx = datetime(2026, 5, 18, 7, 55, 0)
        spectrum_a.stop_dateidx = datetime(2026, 5, 18, 21, 50, 0)
        spectrum_b.stop_dateidx = datetime(2026, 5, 19, 12, 0, 0)
        bin_data = {
            "method": "Fixed logger",
            "spectrum": [spectrum_a, spectrum_b],
        }

        spectrum_ids = processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="rfeye002083",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(spectrum_ids, [801, 801])
        self.assertEqual(len(db.insert_spectrum_calls), 1)

    def test_insert_spectra_batch_keeps_time_end_distinct_for_non_rfeye(self) -> None:
        db = FakeDbRfmIngest()
        spectrum_a = self._build_spectrum(site_id=10, equipment_name="keysight a")
        spectrum_b = self._build_spectrum(site_id=10, equipment_name="keysight a")
        spectrum_a.description = "PMRD (Faixa 4 de 4)."
        spectrum_b.description = "PMRD (Faixa 4 de 4)."
        spectrum_a.start_mega = spectrum_b.start_mega = 470.0
        spectrum_a.stop_mega = spectrum_b.stop_mega = 700.0
        spectrum_a.ndata = spectrum_b.ndata = 5888
        spectrum_a.trace_length = 168
        spectrum_b.trace_length = 338
        spectrum_a.start_dateidx = spectrum_b.start_dateidx = datetime(2026, 5, 18, 7, 55, 0)
        spectrum_a.stop_dateidx = datetime(2026, 5, 18, 21, 50, 0)
        spectrum_b.stop_dateidx = datetime(2026, 5, 19, 12, 0, 0)
        bin_data = {
            "method": "Drive test",
            "spectrum": [spectrum_a, spectrum_b],
        }

        spectrum_ids = processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="keysight_mobile",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(spectrum_ids, [801, 802])
        self.assertEqual(len(db.insert_spectrum_calls), 2)

    def test_insert_spectra_batch_rejects_malformed_cwsm_receiver(self) -> None:
        db = FakeDbRfmIngest()
        spectrum = self._build_spectrum(site_id=10, equipment_name="cwsm2110000")
        bin_data = {
            "method": "Fixed logger",
            "spectrum": [spectrum],
        }

        with self.assertRaises(worker.errors.BinValidationError):
            processing.insert_spectra_batch(
                db_rfm=db,
                bin_data=bin_data,
                hostname_db="CWSM211005",
                host_path="/host/path",
                host_file_name="source.zip",
                extension=".zip",
                vl_file_size_kb=1,
                dt_created=datetime(2026, 1, 1, 12, 0, 0),
                dt_modified=datetime(2026, 1, 1, 12, 0, 0),
            )

        self.assertEqual(
            db.equipment_calls,
            [],
        )

    def test_insert_spectra_batch_accepts_seven_digit_cwsm_receiver(self) -> None:
        db = FakeDbRfmIngest()
        spectrum = self._build_spectrum(site_id=10, equipment_name="CWSM2110021")
        bin_data = {
            "method": "Fixed logger",
            "spectrum": [spectrum],
        }

        processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="CWSM2110021",
            host_path="/host/path",
            host_file_name="source.zip",
            extension=".zip",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(
            db.equipment_calls,
            [
                {
                    "name": "cwsm21100021",
                    "type_hint": "cwsm21100021",
                }
            ],
        )

    def test_insert_spectra_batch_uses_host_identity_and_receiver_type_for_ermx(self) -> None:
        db = FakeDbRfmIngest()
        spectrum = self._build_spectrum(
            site_id=10,
            equipment_name="TEKTRONIX,SA2500,B040241,7.041",
        )
        bin_data = {
            "method": "Fixed logger",
            "spectrum": [spectrum],
        }

        processing.insert_spectra_batch(
            db_rfm=db,
            bin_data=bin_data,
            hostname_db="ERMxES03",
            host_path="/host/path",
            host_file_name="source.bin",
            extension=".bin",
            vl_file_size_kb=1,
            dt_created=datetime(2026, 1, 1, 12, 0, 0),
            dt_modified=datetime(2026, 1, 1, 12, 0, 0),
        )

        self.assertEqual(
            db.equipment_calls,
            [
                {
                    "name": "ermxes03",
                    "type_hint": "TEKTRONIX,SA2500,B040241,7.041",
                }
            ],
        )


class FileMetadataTests(unittest.TestCase):
    """Validate file metadata helpers in processing module."""

    def test_build_history_metadata_from_file_meta_returns_canonical_fields(self) -> None:
        created = datetime(2026, 3, 16, 12, 0, 0)
        file_meta = {
            "file_name": "sample_DONE.mat",
            "extension": ".mat",
            "size_kb": 42,
            "dt_created": created,
            "dt_modified": created,
        }

        history = processing.build_history_metadata_from_file_meta(file_meta)

        self.assertEqual(history["name"], "sample_DONE.mat")
        self.assertEqual(history["extension"], ".mat")
        self.assertEqual(history["size_kb"], 42)

    def test_build_history_metadata_from_file_meta_preserves_timestamps(self) -> None:
        created = datetime(2026, 3, 16, 12, 0, 0)
        modified = datetime(2026, 3, 17, 8, 0, 0)
        file_meta = {
            "file_name": "out.mat",
            "extension": ".mat",
            "size_kb": 10,
            "dt_created": created,
            "dt_modified": modified,
        }

        history = processing.build_history_metadata_from_file_meta(file_meta)

        self.assertEqual(history["dt_created"], created)
        self.assertEqual(history["dt_modified"], modified)

    def test_is_same_file_normalizes_equivalent_paths(self) -> None:
        file_a = {"full_path": "/mnt/reposfi/tmp/../tmp/file.zip"}
        file_b = {"full_path": "/mnt/reposfi/tmp/file.zip"}

        self.assertTrue(processing.is_same_file(file_a, file_b))

    def test_is_same_file_rejects_missing_metadata(self) -> None:
        self.assertFalse(processing.is_same_file(None, {"full_path": "/tmp/file.zip"}))


class FileMoveTests(unittest.TestCase):
    """Validate helper moves between inbox, trash and resolved_files."""

    def test_move_file_if_present_moves_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"
            source_dir.mkdir()
            source_file = source_dir / "sample_DONE.mat"
            source_file.write_text("payload", encoding="utf-8")

            file_meta = {
                "file_path": str(source_dir),
                "file_name": source_file.name,
                "extension": ".mat",
                "size_kb": 1,
                "dt_created": datetime.now(),
                "dt_modified": datetime.now(),
                "full_path": str(source_file),
            }

            moved = processing.move_file_if_present(file_meta, str(target_dir))

            self.assertIsNotNone(moved)
            self.assertEqual(moved["file_path"], str(target_dir))
            self.assertTrue((target_dir / source_file.name).exists())
            self.assertFalse(source_file.exists())

    def test_move_file_if_present_logs_successful_move(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"
            source_dir.mkdir()
            source_file = source_dir / "sample_DONE.mat"
            source_file.write_text("payload", encoding="utf-8")
            fake_log = FakeWorkerLog()

            file_meta = {
                "file_path": str(source_dir),
                "file_name": source_file.name,
                "extension": ".mat",
                "size_kb": 1,
                "dt_created": datetime.now(),
                "dt_modified": datetime.now(),
                "full_path": str(source_file),
            }

            moved = processing.move_file_if_present(
                file_meta,
                str(target_dir),
                logger=fake_log,
            )

            self.assertIsNotNone(moved)
            self.assertEqual(fake_log.entries[-1][0], "file_move")
            self.assertEqual(fake_log.entries[-1][1]["file"], source_file.name)
            self.assertEqual(
                fake_log.entries[-1][1]["source_dir"],
                str(source_dir),
            )
            self.assertEqual(fake_log.entries[-1][1]["destiny_dir"], str(target_dir))
            self.assertTrue(fake_log.entries[-1][1]["success"])

    def test_move_file_if_present_ignores_absent_file(self) -> None:
        file_meta = {
            "file_path": "/tmp",
            "file_name": "missing.mat",
            "extension": ".mat",
            "size_kb": 1,
            "dt_created": datetime.now(),
            "dt_modified": datetime.now(),
            "full_path": "/tmp/missing.mat",
        }

        self.assertIsNone(processing.move_file_if_present(file_meta, "/tmp/target"))

    def test_move_file_if_present_can_refresh_mtime_for_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"
            source_dir.mkdir()
            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("payload", encoding="utf-8")

            original_ts = 946684800  # 2000-01-01 00:00:00 UTC
            os.utime(source_file, (original_ts, original_ts))

            file_meta = {
                "file_path": str(source_dir),
                "file_name": source_file.name,
                "extension": ".zip",
                "size_kb": 1,
                "dt_created": datetime.now(),
                "dt_modified": datetime.now(),
                "full_path": str(source_file),
            }

            moved = processing.move_file_if_present(
                file_meta,
                str(target_dir),
                refresh_mtime=True,
            )

            moved_file = target_dir / source_file.name
            self.assertIsNotNone(moved)
            self.assertTrue(moved_file.exists())
            self.assertGreater(moved_file.stat().st_mtime, original_ts + 60)

    def test_move_file_if_present_logs_failed_move(self) -> None:
        fake_log = FakeWorkerLog()
        file_meta = {
            "file_path": "/tmp",
            "file_name": "broken.mat",
            "extension": ".mat",
            "size_kb": 1,
            "dt_created": datetime.now(),
            "dt_modified": datetime.now(),
            "full_path": "/tmp/broken.mat",
        }

        with patch.object(processing.os.path, "exists", return_value=True):
            with patch.object(
                processing,
                "file_move",
                side_effect=OSError(errno.EIO, "simulated failure"),
            ):
                with self.assertRaises(OSError):
                    processing.move_file_if_present(
                        file_meta,
                        "/tmp/target",
                        logger=fake_log,
                    )

        self.assertEqual(fake_log.errors[-1][0], "file_move")
        self.assertEqual(fake_log.errors[-1][1]["file"], "broken.mat")
        self.assertEqual(fake_log.errors[-1][1]["source_dir"], "/tmp")
        self.assertEqual(fake_log.errors[-1][1]["destiny_dir"], "/tmp/target")
        self.assertEqual(fake_log.errors[-1][1]["source"], "/tmp/broken.mat")
        self.assertEqual(
            fake_log.errors[-1][1]["destiny"],
            "/tmp/target/broken.mat",
        )
        self.assertFalse(fake_log.errors[-1][1]["success"])

    def test_file_move_retries_transient_ebusy_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"
            source_dir.mkdir()
            source_file = source_dir / "sample_DONE.bin"
            source_file.write_text("payload", encoding="utf-8")

            real_rename = os.rename
            rename_calls = []
            sleep_calls = []

            def flaky_rename(source, target):
                rename_calls.append((source, target))
                if len(rename_calls) == 1:
                    raise OSError(errno.EBUSY, "Device or resource busy")
                return real_rename(source, target)

            with patch.object(processing.os, "rename", side_effect=flaky_rename):
                with patch.object(processing.time, "sleep", side_effect=sleep_calls.append):
                    result = processing.file_move(
                        filename=source_file.name,
                        path=str(source_dir),
                        new_path=str(target_dir),
                    )

            self.assertEqual(result["path"], str(target_dir))
            self.assertEqual(len(rename_calls), 2)
            self.assertEqual(sleep_calls, [0.5])
            self.assertTrue((target_dir / source_file.name).exists())
            self.assertFalse(source_file.exists())


class RetryTests(unittest.TestCase):
    """Validate retry-only paths that must preserve the claimed FILE_TASK row."""

    def setUp(self) -> None:
        self.app = worker.AppAnaliseConnection()
        self.app._outage_tracker.reset()

    def test_preflight_app_analise_connection_returns_false_without_claiming_task(self) -> None:
        fake_log = FakeWorkerLog()

        with patch.object(
            self.app,
            "check_connection",
            side_effect=worker.errors.ExternalServiceTransientError("service down"),
        ):
            self.assertFalse(self.app.check_connection_with_log(fake_log))

        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])

    def test_preflight_app_analise_connection_throttles_repeated_identical_outage_logs(self) -> None:
        fake_log = FakeWorkerLog()

        with patch.object(
            self.app,
            "check_connection",
            side_effect=worker.errors.ExternalServiceTransientError("Connection refused"),
        ):
            with patch.object(
                self.app._outage_tracker,
                "_get_monotonic",
                side_effect=[100.0, 110.0, 401.0],
            ):
                self.assertFalse(self.app.check_connection_with_log(fake_log))
                self.assertFalse(self.app.check_connection_with_log(fake_log))
                self.assertFalse(self.app.check_connection_with_log(fake_log))

        self.assertEqual(len(fake_log.warnings), 2)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])
        self.assertIn("Connection refused", fake_log.warnings[0])
        self.assertIn("appanalise_unavailable_still_down", fake_log.warnings[1])
        self.assertIn("suppressed_retries=1", fake_log.warnings[1])

    def test_preflight_app_analise_connection_logs_recovery_after_suppressed_failures(self) -> None:
        fake_log = FakeWorkerLog()
        side_effects = [
            worker.errors.ExternalServiceTransientError("Connection refused"),
            worker.errors.ExternalServiceTransientError("Connection refused"),
            None,
        ]

        with patch.object(self.app, "check_connection", side_effect=side_effects):
            with patch.object(
                self.app._outage_tracker,
                "_get_monotonic",
                side_effect=[200.0, 210.0, 260.0],
            ):
                self.assertFalse(self.app.check_connection_with_log(fake_log))
                self.assertFalse(self.app.check_connection_with_log(fake_log))
                self.assertTrue(self.app.check_connection_with_log(fake_log))

        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])
        self.assertEqual(len(fake_log.entries), 1)
        self.assertEqual(fake_log.entries[0][0], "appanalise_recovered")
        self.assertEqual(fake_log.entries[0][1]["previous_error"], "Connection refused")
        self.assertEqual(fake_log.entries[0][1]["suppressed_retries_total"], 1)

    def test_finalize_freeze_freezes_timeout_row_and_history(self) -> None:
        db = FakeDbBkp()
        task = {
            "file_task_id": 321,
            "host_id": 77,
            "host_file_name": "sample.bin",
            "host_path": "/host/path",
            "filename": "sample.bin",
        }
        err = FakeErr("[ERROR] timeout", triggered=True)
        err.exc = worker.errors.AppAnaliseReadTimeoutError("timeout")

        worker._finalize_freeze(db, task, err)

        self.assertEqual(len(db.task_updates), 1)
        self.assertEqual(db.task_updates[0]["NU_STATUS"], worker.k.TASK_FROZEN)
        self.assertIsNone(db.task_updates[0]["NU_PID"])
        self.assertIn("Processing Frozen", db.task_updates[0]["NA_MESSAGE"])
        self.assertIn("frozen for manual review", db.task_updates[0]["NA_MESSAGE"])

        self.assertEqual(len(db.history_updates), 1)
        self.assertEqual(
            db.history_updates[0]["NU_STATUS_PROCESSING"],
            worker.k.TASK_FROZEN,
        )
        self.assertNotIn("DT_PROCESSED", db.history_updates[0])

        self.assertEqual(len(db.task_deletes), 0)
        self.assertEqual(len(db.statistics_updates), 1)

    def test_finalize_freeze_freezes_file_unavailable_row_and_history(self) -> None:
        db = FakeDbBkp()
        task = {
            "file_task_id": 654,
            "host_id": 88,
            "host_file_name": "sample.bin",
            "host_path": "/host/path",
            "filename": "sample.bin",
        }
        err = FakeErr("[ERROR] file unavailable", triggered=True)
        err.exc = worker.errors.AppAnaliseFileUnavailableError("file unavailable")

        worker._finalize_freeze(db, task, err)

        self.assertEqual(len(db.task_updates), 1)
        self.assertEqual(db.task_updates[0]["NU_STATUS"], worker.k.TASK_FROZEN)
        self.assertEqual(len(db.history_updates), 1)
        self.assertEqual(
            db.history_updates[0]["NU_STATUS_PROCESSING"],
            worker.k.TASK_FROZEN,
        )
        self.assertIn("APP_ANALISE file unavailable", db.task_updates[0]["NA_MESSAGE"])
        self.assertEqual(len(db.statistics_updates), 1)

    def test_finalize_freeze_freezes_service_response_row_and_history(self) -> None:
        db = FakeDbBkp()
        task = {
            "file_task_id": 655,
            "host_id": 89,
            "host_file_name": "sample.bin",
            "host_path": "/host/path",
            "filename": "sample.bin",
        }
        err = FakeErr("[ERROR] service response", triggered=True)
        err.exc = worker.errors.AppAnaliseServiceResponseError(
            "APP_ANALISE returned error in Answer: "
            "server:SSHHandler:AuthenticationFailed"
        )

        worker._finalize_freeze(db, task, err)

        self.assertEqual(len(db.task_updates), 1)
        self.assertEqual(db.task_updates[0]["NU_STATUS"], worker.k.TASK_FROZEN)
        self.assertEqual(len(db.history_updates), 1)
        self.assertEqual(
            db.history_updates[0]["NU_STATUS_PROCESSING"],
            worker.k.TASK_FROZEN,
        )
        self.assertIn(
            "APP_ANALISE returned service error",
            db.task_updates[0]["NA_MESSAGE"],
        )
        self.assertEqual(len(db.task_deletes), 0)
        self.assertEqual(len(db.statistics_updates), 1)

    def test_finalize_error_routes_empty_spec_data_to_error_writer(self) -> None:
        task = {
            "file_task_id": 656,
            "host_id": 90,
            "host_file_name": "sample.bin",
            "host_path": "/host/path",
            "filename": "sample.bin",
        }
        err = FakeErr("[ERROR] empty spec data", triggered=True)
        err.exc = worker.errors.AppAnaliseServiceResponseError(
            "APP_ANALISE returned error in Answer: "
            "handlers:FileReadHandler:EmptySpecData"
        )

        with patch.object(worker, "_write_task_error") as write_error:
            with patch.object(worker, "_finalize_freeze") as finalize_freeze:
                worker._finalize_error(FakeDbBkp(), task, err)

        write_error.assert_called_once()
        finalize_freeze.assert_not_called()

    def test_finalize_error_routes_no_readable_files_to_error_writer(self) -> None:
        task = {
            "file_task_id": 657,
            "host_id": 91,
            "host_file_name": "sample.zip",
            "host_path": "/host/path",
            "filename": "sample.zip",
        }
        err = FakeErr("[ERROR] no readable files", triggered=True)
        err.exc = worker.errors.AppAnaliseServiceResponseError(
            "APP_ANALISE returned error in Answer: "
            "model:SpecDataBase:NoReadableFilesInZip"
        )

        with patch.object(worker, "_write_task_error") as write_error:
            with patch.object(worker, "_finalize_freeze") as finalize_freeze:
                worker._finalize_error(FakeDbBkp(), task, err)

        write_error.assert_called_once()
        finalize_freeze.assert_not_called()


class PathRuleTests(unittest.TestCase):
    """Validate derived repository locations used by the worker helpers."""

    def test_build_resolved_files_trash_path_uses_dedicated_subdir(self) -> None:
        resolved_trash = processing.build_resolved_files_trash_path()

        self.assertTrue(resolved_trash.endswith("/trash/resolved_files"))
        self.assertIn(worker.k.REPO_FOLDER, resolved_trash)


class WorkerFlowScenarioTests(unittest.TestCase):
    """Exercise larger worker scenarios around appAnalise finalization rules."""

    def test_main_does_not_read_file_task_when_appanalise_is_unavailable(self) -> None:
        fake_log = FakeWorkerLog()
        read_calls = []
        sleep_calls = []

        class FakeDbBkpMain:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def read_file_task(self, **kwargs):
                read_calls.append(kwargs)
                return None

        class FakeDbRfmMain:
            def __init__(self, *args, **kwargs) -> None:
                self.in_transaction = False

        class FakeApp(worker.AppAnaliseConnection):
            def check_connection(self) -> None:
                worker.process_status["running"] = False
                raise worker.errors.ExternalServiceTransientError("service down")

        with patch.object(worker, "log", fake_log):
            with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                    with patch.object(worker, "AppAnaliseConnection", FakeApp):
                        with patch.object(
                            worker.runtime_sleep,
                            "random_jitter_sleep",
                            side_effect=lambda: sleep_calls.append("slept"),
                        ):
                            worker.process_status["running"] = True
                            worker.main()

        self.assertEqual(read_calls, [])
        self.assertEqual(sleep_calls, ["slept"])
        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])

    def test_main_skips_processing_when_another_worker_wins_the_claim(self) -> None:
        fake_log = FakeWorkerLog()
        sleep_calls = []

        class FakeDbBkpMain(FakeDbBkp):
            last_instance = None

            def __init__(self, *args, **kwargs) -> None:
                super().__init__()
                self._read_once = False
                FakeDbBkpMain.last_instance = self

            def read_file_task(self, **kwargs):
                if self._read_once:
                    return None
                self._read_once = True
                return (
                    {
                        "FILE_TASK__ID_FILE_TASK": 321,
                        "FILE_TASK__NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEye002211",
                        "FILE_TASK__NA_SERVER_FILE_NAME": "sample.bin",
                        "FILE_TASK__NA_HOST_FILE_PATH": "/mnt/internal/data/2026/PECAN",
                        "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
                        "HOST__NA_HOST_NAME": "RFEye002211",
                        "FILE_TASK__NA_EXTENSION": ".bin",
                        "FILE_TASK__DT_FILE_CREATED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__DT_FILE_MODIFIED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__VL_FILE_SIZE_KB": 123,
                    },
                    10699,
                    None,
                )

            def file_task_update(self, **kwargs):
                self.task_updates.append(kwargs)
                return {
                    "success": True,
                    "rows_affected": 0,
                    "updated_fields": kwargs,
                }

        class FakeDbRfmMain:
            def __init__(self, *args, **kwargs) -> None:
                self.in_transaction = False

        class FakeApp(worker.AppAnaliseConnection):
            def check_connection(self) -> None:
                return None

            def process(self, **kwargs):
                raise AssertionError("process() should not run after a lost claim")

        def stop_after_second_sleep():
            sleep_calls.append("slept")
            if len(sleep_calls) >= 2:
                worker.process_status["running"] = False

        with patch.object(worker, "log", fake_log):
            with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                    with patch.object(worker, "AppAnaliseConnection", FakeApp):
                        with patch.object(
                            worker.runtime_sleep,
                            "random_jitter_sleep",
                            side_effect=stop_after_second_sleep,
                        ):
                            worker.process_status["running"] = True
                            worker.main()

        db_bp = FakeDbBkpMain.last_instance
        self.assertEqual(len(db_bp.task_updates), 1)
        self.assertEqual(db_bp.task_updates[0]["expected_status"], worker.k.TASK_PENDING)
        self.assertEqual(len(db_bp.task_deletes), 0)
        self.assertEqual(len(db_bp.history_updates), 0)
        self.assertEqual(len(db_bp.statistics_updates), 0)
        self.assertEqual(sleep_calls, ["slept", "slept"])
        self.assertTrue(
            any("event=file_task_claim_lost" in warning for warning in fake_log.warnings)
        )

    def test_main_uses_export_as_error_artifact_when_validation_fails_after_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("zip payload", encoding="utf-8")
            exported_file = source_dir / "sample_DONE.mat"
            exported_file.write_text("mat payload", encoding="utf-8")

            exported_meta = build_test_file_meta(exported_file)
            fake_log = FakeWorkerLog()

            row = {
                "FILE_TASK__ID_FILE_TASK": 321,
                "FILE_TASK__NA_SERVER_FILE_PATH": str(source_dir),
                "FILE_TASK__NA_SERVER_FILE_NAME": source_file.name,
                "FILE_TASK__NA_HOST_FILE_PATH": "/host/path",
                "FILE_TASK__NA_HOST_FILE_NAME": "host_sample.zip",
                "HOST__NA_HOST_NAME": "CWSM21100001",
                "FILE_TASK__NA_EXTENSION": ".zip",
                "FILE_TASK__DT_FILE_CREATED": exported_meta["dt_created"],
                "FILE_TASK__DT_FILE_MODIFIED": exported_meta["dt_modified"],
                "FILE_TASK__VL_FILE_SIZE_KB": 1,
            }

            class FakeDbBkpMain(FakeDbBkp):
                def __init__(self, *args, **kwargs) -> None:
                    super().__init__()
                    self._read_once = False

                def read_file_task(self, **kwargs):
                    if self._read_once:
                        return None
                    self._read_once = True
                    return row, 77, None

            class FakeDbRfmMain:
                def __init__(self, *args, **kwargs) -> None:
                    self.in_transaction = False

            class FakeApp(worker.AppAnaliseConnection):
                def __init__(self) -> None:
                    super().__init__()

                def check_connection(self) -> None:
                    return None

                def process(self, **kwargs):
                    self.last_output_meta = dict(exported_meta)
                    raise worker.errors.BinValidationError(
                        "Invalid GPS reading: GNSS unavailable sentinel"
                    )

            sleep_calls = []

            def stop_after_iteration():
                sleep_calls.append("slept")
                worker.process_status["running"] = False

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                with patch.object(worker.k, "TRASH_FOLDER", "trash"):
                    with patch.object(worker, "log", fake_log):
                        with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                            with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                                with patch.object(worker, "AppAnaliseConnection", FakeApp):
                                    with patch.object(
                                        worker.runtime_sleep,
                                        "random_jitter_sleep",
                                        side_effect=stop_after_iteration,
                                    ):
                                        worker.process_status["running"] = True
                                        worker.main()

            resolved_source = (
                repo_root / "trash" / "resolved_files" / source_file.name
            )
            trashed_export = repo_root / "trash" / exported_file.name

            # Once appAnalise has already produced the `.mat`, the RF.Fusion
            # validation failure should treat that export as the canonical
            # error artifact and retire the original `.zip`.
            self.assertEqual(sleep_calls, ["slept"])
            self.assertTrue(resolved_source.exists())
            self.assertTrue(trashed_export.exists())
            self.assertFalse(source_file.exists())
            self.assertFalse(exported_file.exists())

    def test_main_requeues_when_final_filesystem_promotion_is_transient(self) -> None:
        fake_log = FakeWorkerLog()
        sleep_calls = []

        class FakeDbBkpMain(FakeDbBkp):
            last_instance = None

            def __init__(self, *args, **kwargs) -> None:
                super().__init__()
                self._read_once = False
                FakeDbBkpMain.last_instance = self

            def read_file_task(self, **kwargs):
                if self._read_once:
                    return None
                self._read_once = True
                return (
                    {
                        "FILE_TASK__ID_FILE_TASK": 321,
                        "FILE_TASK__NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEye002211",
                        "FILE_TASK__NA_SERVER_FILE_NAME": "sample.bin",
                        "FILE_TASK__NA_HOST_FILE_PATH": "/mnt/internal/data/2026/PECAN",
                        "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
                        "HOST__NA_HOST_NAME": "RFEye002211",
                        "FILE_TASK__NA_EXTENSION": ".bin",
                        "FILE_TASK__DT_FILE_CREATED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__DT_FILE_MODIFIED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__VL_FILE_SIZE_KB": 123,
                    },
                    10699,
                    None,
                )

        class FakeDbRfmMain:
            last_instance = None

            def __init__(self, *args, **kwargs) -> None:
                self.in_transaction = False
                self.commit_calls = 0
                self.rollback_calls = 0
                FakeDbRfmMain.last_instance = self

            def build_path(self, site_id: int) -> str:
                return f"site_{site_id}/catalog"

            def begin_transaction(self) -> None:
                self.in_transaction = True

            def commit(self) -> None:
                self.commit_calls += 1
                self.in_transaction = False

            def rollback(self) -> None:
                self.rollback_calls += 1
                self.in_transaction = False

        class FakeApp(worker.AppAnaliseConnection):
            def check_connection(self) -> None:
                return None

            def process(self, **kwargs):
                spectrum = SimpleNamespace(
                    site_data={
                        "longitude": -51.23,
                        "latitude": -30.01,
                        "altitude": 10.0,
                        "longitude_raw": [-51.23],
                        "latitude_raw": [-30.01],
                        "altitude_raw": [10.0],
                        "nu_gnss_measurements": 1,
                        "geographic_path": None,
                    },
                    start_dateidx=datetime(2026, 2, 4, 7, 24, 15),
                    site_id=219,
                )
                return (
                    {"method": "Fixed logger", "spectrum": [spectrum]},
                    {
                        "file_path": "/mnt/reposfi/tmp/RFEye002211",
                        "file_name": "sample.bin",
                        "extension": ".bin",
                        "size_kb": 123,
                        "dt_created": datetime(2026, 2, 4, 7, 24, 15),
                        "dt_modified": datetime(2026, 2, 4, 7, 24, 15),
                        "full_path": "/mnt/reposfi/tmp/RFEye002211/sample.bin",
                    },
                )

        def stop_after_iteration():
            sleep_calls.append("slept")
            worker.process_status["running"] = False

        with patch.object(worker, "log", fake_log):
            with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                    with patch.object(worker, "AppAnaliseConnection", FakeApp):
                        with patch.object(
                            worker.processing,
                            "resolve_spectrum_sites",
                            return_value=[219],
                        ):
                            with patch.object(
                                worker.processing,
                                "insert_spectra_batch",
                                return_value=[9001],
                            ):
                                with patch.object(
                                    worker.processing,
                                    "promote_final_artifact",
                                    side_effect=OSError(
                                        errno.EBUSY,
                                        "Device or resource busy",
                                    ),
                                ):
                                    with patch.object(
                                        worker.runtime_sleep,
                                        "random_jitter_sleep",
                                        side_effect=stop_after_iteration,
                                    ):
                                        worker.process_status["running"] = True
                                        worker.main()

        db_bp = FakeDbBkpMain.last_instance
        db_rfm = FakeDbRfmMain.last_instance

        self.assertEqual(db_rfm.commit_calls, 1)
        self.assertEqual(len(db_bp.task_updates), 2)
        self.assertEqual(db_bp.task_updates[0]["NU_STATUS"], worker.k.TASK_RUNNING)
        self.assertEqual(db_bp.task_updates[1]["NU_STATUS"], worker.k.TASK_FROZEN)
        self.assertIsNone(db_bp.task_updates[1]["NU_PID"])
        self.assertIn(
            "Transient filesystem finalization failure",
            db_bp.task_updates[1]["NA_MESSAGE"],
        )
        self.assertEqual(len(db_bp.task_deletes), 0)
        self.assertEqual(len(db_bp.history_updates), 1)
        self.assertEqual(
            db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
            worker.k.TASK_FROZEN,
        )
        self.assertEqual(len(db_bp.statistics_updates), 1)
        self.assertEqual(sleep_calls, ["slept"])
        self.assertTrue(
            any(
                isinstance(item, tuple) and item[0] == "processing_frozen"
                for item in fake_log.errors
            )
        )

    def test_main_freezes_task_when_appanalise_returns_structured_read_timeout(self) -> None:
        fake_log = FakeWorkerLog()
        sleep_calls = []

        class FakeDbBkpMain(FakeDbBkp):
            last_instance = None

            def __init__(self, *args, **kwargs) -> None:
                super().__init__()
                self._read_once = False
                FakeDbBkpMain.last_instance = self

            def read_file_task(self, **kwargs):
                if self._read_once:
                    return None
                self._read_once = True
                return (
                    {
                        "FILE_TASK__ID_FILE_TASK": 321,
                        "FILE_TASK__NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEye002211",
                        "FILE_TASK__NA_SERVER_FILE_NAME": "sample.bin",
                        "FILE_TASK__NA_HOST_FILE_PATH": "/mnt/internal/data/2026/PECAN",
                        "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
                        "HOST__NA_HOST_NAME": "RFEye002211",
                        "FILE_TASK__NA_EXTENSION": ".bin",
                        "FILE_TASK__DT_FILE_CREATED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__DT_FILE_MODIFIED": datetime(2026, 2, 4, 7, 24, 15),
                        "FILE_TASK__VL_FILE_SIZE_KB": 123,
                    },
                    10699,
                    None,
                )

        class FakeDbRfmMain:
            def __init__(self, *args, **kwargs) -> None:
                self.in_transaction = False

        class FakeApp(worker.AppAnaliseConnection):
            def check_connection(self) -> None:
                return None

            def process(self, **kwargs):
                raise worker.errors.AppAnaliseReadTimeoutError(
                    "APP_ANALISE returned FileRead timeout: "
                    "handlers:FileReadHandler:ReadTimeout"
                )

        def stop_after_iteration():
            sleep_calls.append("slept")
            worker.process_status["running"] = False

        with patch.object(worker, "log", fake_log):
            with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                    with patch.object(worker, "AppAnaliseConnection", FakeApp):
                        with patch.object(
                            worker.runtime_sleep,
                            "random_jitter_sleep",
                            side_effect=stop_after_iteration,
                        ):
                            worker.process_status["running"] = True
                            worker.main()

        db_bp = FakeDbBkpMain.last_instance

        self.assertEqual(len(db_bp.task_updates), 2)
        self.assertEqual(db_bp.task_updates[0]["NU_STATUS"], worker.k.TASK_RUNNING)
        self.assertEqual(db_bp.task_updates[1]["NU_STATUS"], worker.k.TASK_FROZEN)
        self.assertIsNone(db_bp.task_updates[1]["NU_PID"])
        self.assertIn("Processing Frozen", db_bp.task_updates[1]["NA_MESSAGE"])
        self.assertEqual(len(db_bp.task_deletes), 0)
        self.assertEqual(len(db_bp.history_updates), 1)
        self.assertEqual(
            db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
            worker.k.TASK_FROZEN,
        )
        self.assertEqual(len(db_bp.statistics_updates), 1)
        self.assertEqual(sleep_calls, ["slept"])
        self.assertTrue(
            any(
                isinstance(item, tuple) and item[0] == "processing_frozen"
                for item in fake_log.errors
            )
        )

    def test_successful_export_promotes_mat_and_retires_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("zip payload", encoding="utf-8")
            exported_file = source_dir / "sample_DONE.mat"
            exported_file.write_text("mat payload", encoding="utf-8")

            source_meta = build_test_file_meta(source_file)
            exported_meta = build_test_file_meta(exported_file)
            db_rfm = FakeDbRfm()
            db_bp = FakeDbBkp()
            fake_log = FakeWorkerLog()

            bin_data = {
                "spectrum": [
                    SimpleNamespace(
                        start_dateidx=datetime(2026, 1, 31, 20, 18, 51),
                        site_id=5,
                    )
                ]
            }
            hostname_db = "CWSM21100001"
            spectrum_ids = [10, 11]

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                with patch.object(worker.k, "TRASH_FOLDER", "trash"):
                    with patch.object(worker, "log", fake_log):
                        # Step 1: resolve destination path (DB read, no write)
                        new_path = processing.build_repository_destination_path(
                            db_rfm=db_rfm,
                            bin_data=bin_data,
                            hostname_db=hostname_db,
                        )
                        # Step 2: pure filesystem — move artifact, retire source
                        final_meta = processing.promote_final_artifact(
                            new_path=new_path,
                            file_meta=exported_meta,
                            source_file_meta=source_meta,
                            export=True,
                            filename=str(source_file),
                            logger=fake_log,
                        )
                        # Step 3: register the moved artifact in RFDATA
                        server_file_id = db_rfm.insert_file(
                            hostname=hostname_db,
                            NA_VOLUME=worker.k.REPO_VOLUME_NAME,
                            NA_PATH=new_path,
                            NA_FILE=final_meta["file_name"],
                            NA_EXTENSION=final_meta["extension"],
                            VL_FILE_SIZE_KB=final_meta["size_kb"],
                            DT_FILE_CREATED=final_meta["dt_created"],
                            DT_FILE_MODIFIED=final_meta["dt_modified"],
                            log_success=False,
                        )
                        db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])

                        # Step 4: close queue row as DONE in BKP
                        task = {
                            "file_task_id": 99,
                            "host_id": 7,
                            "host_file_name": "host_sample.zip",
                            "host_path": "/host/path",
                            "filename": "host_sample.zip",
                        }
                        result_data = {
                            "file_meta": final_meta,
                            "new_path": new_path,
                            "bin_data": bin_data,
                            "resolved_site_ids": None,
                            "spectrum_ids": None,
                        }
                        worker._finalize_success(
                            db_bp,
                            task,
                            result_data,
                            elapsed_sec=1.234,
                        )

            final_file = Path(final_meta["full_path"])
            resolved_source = repo_root / "trash" / "resolved_files" / source_file.name

            # Success is the mirror image of the definitive-error contract:
            # the `.mat` becomes canonical and the source is simply retired.
            self.assertTrue(final_file.exists())
            self.assertEqual(final_file.read_text(encoding="utf-8"), "mat payload")
            self.assertTrue(resolved_source.exists())
            self.assertEqual(
                resolved_source.read_text(encoding="utf-8"),
                "zip payload",
            )
            self.assertFalse(source_file.exists())
            self.assertFalse(exported_file.exists())

            self.assertEqual(len(db_rfm.insert_file_calls), 1)
            self.assertEqual(db_rfm.insert_file_calls[0]["NA_FILE"], "sample_DONE.mat")
            self.assertFalse(db_rfm.insert_file_calls[0]["log_success"])
            self.assertEqual(len(db_rfm.bridge_calls), 1)
            self.assertEqual(len(db_bp.task_deletes), 1)
            self.assertEqual(len(db_bp.history_updates), 1)
            self.assertEqual(db_bp.transaction_events, ["begin", "commit"])
            self.assertFalse(db_bp.statistics_updates[0]["log_if_active"])
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_NAME"],
                "sample_DONE.mat",
            )
            self.assertEqual(
                db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
                worker.k.TASK_DONE,
            )
            self.assertTrue(
                any(
                    isinstance(item, tuple) and item[0] == "processing_completed"
                    for item in fake_log.entries
                )
            )

    def test_promote_artifact_uses_neutral_path_for_multi_site_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            exported_file = source_dir / "sample_DONE.mat"
            exported_file.write_text("mat payload", encoding="utf-8")

            exported_meta = build_test_file_meta(exported_file)
            db_rfm = FakeDbRfm()

            bin_data = {
                "spectrum": [
                    SimpleNamespace(
                        start_dateidx=datetime(2026, 1, 31, 20, 18, 51),
                        site_id=5,
                    ),
                    SimpleNamespace(
                        start_dateidx=datetime(2026, 1, 31, 20, 19, 51),
                        site_id=6,
                    ),
                ]
            }

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                new_path = processing.build_repository_destination_path(
                    db_rfm=db_rfm,
                    bin_data=bin_data,
                    hostname_db="EMRx001",
                )
                final_meta = processing.promote_final_artifact(
                    new_path=new_path,
                    file_meta=exported_meta,
                    source_file_meta=exported_meta,
                    export=False,
                    filename=str(exported_file),
                )

            self.assertIn("appanalise_multi_site", new_path)
            self.assertIn("emrx001", new_path)
            self.assertTrue(Path(final_meta["full_path"]).exists())

    def test_definitive_failure_uses_export_as_error_artifact_and_resolves_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("zip payload", encoding="utf-8")
            partial_artifact = source_dir / "sample_DONE.mat"
            partial_artifact.write_text("partial mat", encoding="utf-8")

            source_meta = build_test_file_meta(source_file)
            partial_meta = build_test_file_meta(partial_artifact)
            db_bp = FakeDbBkp()
            fake_log = FakeWorkerLog()
            err = FakeErr("[ERROR] validation failed", triggered=True)
            err.exc = SimpleNamespace(file_meta=partial_meta)

            task = {
                "file_task_id": 101,
                "host_id": 8,
                "host_file_name": "host_sample.zip",
                "host_path": "/host/path",
                "filename": str(source_file),
                "source_file_meta": source_meta,
                "export": True,
                "server_name": source_file.name,
                "extension": ".zip",
                "vl_file_size_kb": source_meta["size_kb"],
                "dt_created": source_meta["dt_created"],
                "dt_modified": source_meta["dt_modified"],
                "server_path": str(source_dir),
            }

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                with patch.object(worker.k, "TRASH_FOLDER", "trash"):
                    with patch.object(worker, "log", fake_log):
                        worker._write_task_error(db_bp, task, err)

            resolved_source = repo_root / "trash" / "resolved_files" / source_file.name
            trashed_artifact = repo_root / "trash" / partial_artifact.name

            self.assertTrue(resolved_source.exists())
            self.assertEqual(
                resolved_source.read_text(encoding="utf-8"),
                "zip payload",
            )
            self.assertTrue(trashed_artifact.exists())
            self.assertEqual(
                trashed_artifact.read_text(encoding="utf-8"),
                "partial mat",
            )
            self.assertFalse(source_file.exists())
            self.assertFalse(partial_artifact.exists())

            self.assertEqual(len(db_bp.task_deletes), 1)
            self.assertEqual(len(db_bp.history_updates), 1)
            self.assertEqual(db_bp.transaction_events, ["begin", "commit"])
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_NAME"],
                "sample_DONE.mat",
            )
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_PATH"],
                str(repo_root / "trash"),
            )
            self.assertEqual(
                db_bp.history_updates[0]["NA_EXTENSION"],
                ".mat",
            )
            self.assertEqual(
                db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
                worker.k.TASK_ERROR,
            )
            self.assertIn("[ERROR] validation failed", db_bp.history_updates[0]["NA_MESSAGE"])
            self.assertEqual(len(db_bp.statistics_updates), 1)

    def test_finalize_success_rolls_back_when_history_update_affects_zero_rows(self) -> None:
        fake_log = FakeWorkerLog()
        db_bp = FakeDbBkp()
        db_bp.history_rows_affected = 0
        task = {
            "file_task_id": 101,
            "host_id": 8,
            "host_file_name": "host_sample.zip",
            "host_path": "/host/path",
            "filename": "sample.zip",
        }
        result = {
            "file_meta": {
                "file_path": "/mnt/reposfi/2026/DF/1/2",
                "file_name": "sample_DONE.mat",
                "extension": ".mat",
                "size_kb": 123,
                "dt_created": datetime(2026, 1, 1, 0, 0, 0),
                "dt_modified": datetime(2026, 1, 1, 0, 0, 0),
                "full_path": "/mnt/reposfi/2026/DF/1/2/sample_DONE.mat",
            },
            "new_path": "/mnt/reposfi/2026/DF/1/2",
            "bin_data": None,
            "resolved_site_ids": None,
            "spectrum_ids": None,
        }

        with patch.object(worker, "log", fake_log):
            worker._finalize_success(db_bp, task, result, elapsed_sec=1.234)

        # _finalize_success swallows the error — check transaction was rolled back
        self.assertEqual(len(db_bp.history_updates), 1)
        self.assertEqual(len(db_bp.task_deletes), 0)
        self.assertEqual(len(db_bp.statistics_updates), 0)
        self.assertEqual(db_bp.transaction_events, ["begin", "rollback"])
        self.assertTrue(any("task_finalization_failed" in str(e) for e in fake_log.errors))


if __name__ == "__main__":
    unittest.main()
