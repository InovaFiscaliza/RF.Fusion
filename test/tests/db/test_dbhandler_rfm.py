"""
Validation tests for `dbHandlerRFM.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db/test_dbhandler_rfm.py -q

What is covered here:
    - site lookup rules for fixed and mobile summaries
    - direct site insert and centroid update rules
    - geographic code resolution and district auto-create
    - repository path and file type resolution
    - idempotent inserts for file and spectrum dimensions
    - bridge inserts, parquet export, and latest-processing lookup
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, import_package_module


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        db_rfm_module = import_package_module("db", DB_ROOT, "dbHandlerRFM")


class FakeLog:
    """Collect handler log output without pulling the production logger."""

    def __init__(self) -> None:
        self.entries: list[str] = []
        self.errors: list[str] = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)


class FakeConnection:
    """Minimal connection double used by non-transactional handler methods."""

    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.autocommit = True

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeCursor:
    """Cursor double that replays pre-seeded fetch batches in order."""

    def __init__(self, fetch_batches=None, *, lastrowid: int = 0) -> None:
        self.fetch_batches = list(fetch_batches or [])
        self.executed: list[tuple[str, tuple | None]] = []
        self.lastrowid = lastrowid
        self.rowcount = 1

    def execute(self, sql, params=None) -> None:
        compact_sql = " ".join(str(sql).split())
        self.executed.append((compact_sql, params))

    def executemany(self, sql, seq_of_params) -> None:
        compact_sql = " ".join(str(sql).split())
        self.executed.append((compact_sql, list(seq_of_params)))

    def fetchall(self):
        if self.fetch_batches:
            return self.fetch_batches.pop(0)
        return []


class DbHandlerRfmBaseTests(unittest.TestCase):
    """Shared factory helpers for direct `dbHandlerRFM` tests."""

    def make_handler(self):
        handler = object.__new__(db_rfm_module.dbHandlerRFM)
        handler.log = FakeLog()
        handler.db_connection = FakeConnection()
        handler.cursor = FakeCursor()
        handler.in_transaction = False
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        return handler


class SiteLookupTests(DbHandlerRfmBaseTests):
    """Validate site matching rules before inserts or updates happen."""

    def test_get_site_id_matches_fixed_site_by_centroid_tolerance(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {
                "ID_SITE": 11,
                "LONGITUDE": -36.543807,
                "LATITUDE": -10.286181,
                "GEOGRAPHIC_PATH": None,
                "DISTANCE": 0.0,
            }
        ]

        site_id = handler.get_site_id(
            {
                "longitude": -36.543807,
                "latitude": -10.286181,
                "geographic_path": None,
            }
        )

        self.assertEqual(site_id, 11)

    def test_get_site_id_requires_same_mobile_path(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {
                "ID_SITE": 12,
                "LONGITUDE": -35.897411,
                "LATITUDE": -7.230131,
                "GEOGRAPHIC_PATH": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "DISTANCE": 0.0,
            }
        ]

        site_id = handler.get_site_id(
            {
                "longitude": -35.897411,
                "latitude": -7.230131,
                "geographic_path": "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
            }
        )

        self.assertFalse(site_id)


class SiteWriteTests(DbHandlerRfmBaseTests):
    """Validate direct site insert/update behavior without a real database."""

    def test_insert_site_builds_fixed_point_insert(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor(lastrowid=123)
        handler._normalize_site_data = lambda data: dict(data)
        handler._get_geographic_codes = lambda **kwargs: (35, 3550308, None)

        site_id = handler.insert_site(
            {
                "longitude": -46.633308,
                "latitude": -23.55052,
                "altitude": 760.0,
                "nu_gnss_measurements": 1,
                "state": "São Paulo",
                "county": "São Paulo",
                "district": None,
                "site_name": "Roof A",
            }
        )

        self.assertEqual(site_id, 123)
        self.assertEqual(handler.db_connection.commits, 1)
        self.assertEqual(len(handler.cursor.executed), 1)
        sql, params = handler.cursor.executed[0]
        self.assertIn("INSERT INTO DIM_SPECTRUM_SITE", sql)
        self.assertIn("POINT(-46.633308 -23.55052)", sql)
        self.assertNotIn("GEOGRAPHIC_PATH", sql)
        self.assertEqual(
            params,
            (760.0, 1, 35, 3550308, None, "Roof A"),
        )

    def test_insert_site_includes_mobile_geographic_path(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor(lastrowid=124)
        handler._normalize_site_data = lambda data: dict(data)
        handler._get_geographic_codes = lambda **kwargs: (25, 2507507, None)

        site_id = handler.insert_site(
            {
                "longitude": -35.897411,
                "latitude": -7.230131,
                "altitude": 12.0,
                "nu_gnss_measurements": 1,
                "state": "Paraíba",
                "county": "Campina Grande",
                "district": None,
                "site_name": None,
                "geographic_path": "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",
            }
        )

        self.assertEqual(site_id, 124)
        sql, params = handler.cursor.executed[0]
        self.assertIn("GEOGRAPHIC_PATH", sql)
        self.assertEqual(
            params,
            (
                12.0,
                1,
                25,
                2507507,
                None,
                "Campina Grande",
                "POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",
            ),
        )

    def test_insert_site_rolls_back_and_wraps_error(self) -> None:
        handler = self.make_handler()
        handler._normalize_site_data = lambda data: dict(data)
        handler._get_geographic_codes = lambda **kwargs: (_ for _ in ()).throw(
            Exception("bad geography")
        )

        with self.assertRaises(Exception) as ctx:
            handler.insert_site(
                {
                    "longitude": -46.633308,
                    "latitude": -23.55052,
                    "altitude": 760.0,
                }
            )

        self.assertEqual(handler.db_connection.rollbacks, 1)
        self.assertIn("Error inserting site in DIM_SPECTRUM_SITE", str(ctx.exception))

    def test_update_site_skips_write_after_gnss_limit(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor()
        handler._select_rows = lambda **kwargs: [
            {
                "LONGITUDE": -46.633308,
                "LATITUDE": -23.55052,
                "NU_ALTITUDE": 760.0,
                "NU_GNSS_MEASUREMENTS": db_rfm_module.k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS,
            }
        ]

        handler.update_site(
            site=77,
            longitude_raw=[-46.6333],
            latitude_raw=[-23.5505],
            altitude_raw=[761.0],
        )

        self.assertEqual(handler.cursor.executed, [])
        self.assertTrue(
            any("No update performed" in entry for entry in handler.log.entries)
        )

    def test_update_site_updates_weighted_centroid(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor()
        handler._select_rows = lambda **kwargs: [
            {
                "LONGITUDE": 20.0,
                "LATITUDE": 10.0,
                "NU_ALTITUDE": 100.0,
                "NU_GNSS_MEASUREMENTS": 2,
            }
        ]

        handler.update_site(
            site=88,
            longitude_raw=[26.0, 30.0],
            latitude_raw=[14.0, 18.0],
            altitude_raw=[104.0, 108.0],
        )

        self.assertEqual(len(handler.cursor.executed), 1)
        sql, params = handler.cursor.executed[0]
        self.assertIn("UPDATE DIM_SPECTRUM_SITE", sql)
        self.assertEqual(params, ("POINT(24.0 13.0)", 103.0, 4, 88))

    def test_update_site_rejects_missing_row(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: []

        with self.assertRaises(Exception) as ctx:
            handler.update_site(
                site=91,
                longitude_raw=[-46.6],
                latitude_raw=[-23.5],
                altitude_raw=[760.0],
            )

        self.assertIn("Error updating site 91", str(ctx.exception))

    def test_refresh_site_geography_updates_admin_fields(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor()
        handler._normalize_site_data = lambda data: dict(data)

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SPECTRUM_SITE" and where == {"ID_SITE": 77}:
                return [
                    {
                        "FK_STATE": 35,
                        "FK_COUNTY": 3550308,
                        "FK_DISTRICT": None,
                        "NA_SITE": "Sao Paulo",
                    }
                ]
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        handler._select_rows = fake_select_rows
        handler._get_geographic_codes = (
            lambda **kwargs: (35, 3550308, 9001)
        )

        result = handler.refresh_site_geography(
            77,
            {
                "state": "São Paulo",
                "county": "São Paulo",
                "district": "Campo Belo",
                "district_candidates": ["Campo Belo"],
            },
            force_create_district=True,
        )

        self.assertEqual(result["action"], "updated")
        self.assertEqual(result["fk_district"], 9001)
        self.assertEqual(len(handler.cursor.executed), 1)
        sql, params = handler.cursor.executed[0]
        self.assertIn("UPDATE DIM_SPECTRUM_SITE SET", sql)
        self.assertEqual(params, [35, 3550308, 9001, "Campo Belo", 77])

    def test_refresh_site_geography_dry_run_reports_pending_district_create(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor()
        handler._normalize_site_data = lambda data: dict(data)

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SPECTRUM_SITE" and where == {"ID_SITE": 77}:
                return [
                    {
                        "FK_STATE": 35,
                        "FK_COUNTY": 3550308,
                        "FK_DISTRICT": None,
                        "NA_SITE": "Campo Belo",
                    }
                ]
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        handler._select_rows = fake_select_rows
        handler._get_geographic_codes = (
            lambda **kwargs: (35, 3550308, None)
        )

        result = handler.refresh_site_geography(
            77,
            {
                "state": "São Paulo",
                "county": "São Paulo",
                "district": "Campo Belo",
                "district_candidates": ["Campo Belo"],
            },
            force_create_district=True,
            dry_run=True,
        )

        self.assertEqual(result["action"], "dry_run")
        self.assertTrue(result["would_create_district"])
        self.assertEqual(handler.cursor.executed, [])


class GeographicCodeTests(DbHandlerRfmBaseTests):
    """Validate deterministic geography resolution before site insert."""

    def test_get_geographic_codes_uses_normalized_state_and_county_names(self) -> None:
        handler = self.make_handler()

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SITE_STATE" and where == {"NA_STATE": "Sao Paulo"}:
                return []
            if table == "DIM_SITE_STATE" and where is None:
                return [
                    {"ID_STATE": 35, "NA_STATE": "São Paulo"},
                    {"ID_STATE": 33, "NA_STATE": "Rio de Janeiro"},
                ]
            if table == "DIM_SITE_COUNTY":
                return [
                    {"ID_COUNTY": 3550308, "NA_COUNTY": "São Paulo"},
                    {"ID_COUNTY": 3509502, "NA_COUNTY": "Campinas"},
                ]
            if table == "DIM_SITE_DISTRICT":
                return []
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        handler._select_rows = fake_select_rows
        handler._insert_row = lambda **kwargs: 9001

        with patch.object(db_rfm_module.k, "SITE_DISTRICT_AUTO_CREATE", True):
            state_id, county_id, district_id = handler._get_geographic_codes(
                {
                    "state": "Sao Paulo",
                    "county": "São Paulo",
                    "district": "Sé",
                }
            )

        self.assertEqual((state_id, county_id, district_id), (35, 3550308, 9001))

    def test_get_geographic_codes_tries_secondary_district_candidate_before_create(self) -> None:
        handler = self.make_handler()

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SITE_STATE" and where == {"NA_STATE": "São Paulo"}:
                return [{"ID_STATE": 35, "NA_STATE": "São Paulo"}]
            if table == "DIM_SITE_COUNTY":
                return [
                    {"ID_COUNTY": 3550308, "NA_COUNTY": "São Paulo"},
                ]
            if table == "DIM_SITE_DISTRICT":
                return [
                    {"ID_DISTRICT": 141, "NA_DISTRICT": "Campo Belo"},
                ]
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        handler._select_rows = fake_select_rows
        handler._insert_row = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("district should be matched before auto-create")
        )

        state_id, county_id, district_id = handler._get_geographic_codes(
            {
                "state": "São Paulo",
                "county": "São Paulo",
                "district": "Brooklin",
                "district_candidates": ["Brooklin", "Campo Belo"],
            },
            force_create_district=True,
        )

        self.assertEqual((state_id, county_id, district_id), (35, 3550308, 141))


class FileDimensionTests(DbHandlerRfmBaseTests):
    """Validate file typing, path building and idempotent file inserts."""

    def test_build_path_uses_state_county_and_site_keys(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {"LC_STATE": "sp", "FK_COUNTY": 3550308, "ID_SITE": 77}
        ]

        built = handler.build_path(site_id=77)

        self.assertEqual(built, "sp/3550308/77")

    def test_get_file_type_id_by_hostname_uses_specific_match(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {"ID_TYPE_FILE": 1, "NA_TYPE_FILE": "RFeye BIN", "NA_EQUIPMENT": "rfeye"},
            {"ID_TYPE_FILE": 2, "NA_TYPE_FILE": "appColeta BIN", "NA_EQUIPMENT": "others"},
        ]

        type_id = handler.get_file_type_id_by_hostname("rfeye002106")

        self.assertEqual(type_id, 1)

    def test_get_file_type_id_by_hostname_falls_back_to_others(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {"ID_TYPE_FILE": 2, "NA_TYPE_FILE": "appColeta BIN", "NA_EQUIPMENT": "others"},
        ]

        type_id = handler.get_file_type_id_by_hostname("unknown_station")

        self.assertEqual(type_id, 2)

    def test_insert_file_returns_existing_repository_artifact(self) -> None:
        handler = self.make_handler()
        handler.get_file_type_id_by_hostname = lambda HOSTNAME: 41

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SPECTRUM_FILE":
                return [{"ID_FILE": 700}]
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        handler._select_rows = fake_select_rows
        handler._insert_row = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("insert should not be called for existing file")
        )

        file_id = handler.insert_file(
            hostname="CWSM21100001",
            NA_VOLUME="REPOSFI",
            NA_PATH="/sp/3550308/77",
            NA_FILE="sample_DONE.mat",
            NA_EXTENSION=".mat",
            VL_FILE_SIZE_KB=10,
        )

        self.assertEqual(file_id, 700)

    def test_insert_file_inserts_new_artifact_with_lowercase_volume(self) -> None:
        handler = self.make_handler()
        inserted = {}
        handler.get_file_type_id_by_hostname = lambda HOSTNAME: 41
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data, **kwargs):
            inserted["table"] = table
            inserted["data"] = data
            return 701

        handler._insert_row = fake_insert_row

        file_id = handler.insert_file(
            hostname="CWSM21100001",
            NA_VOLUME="REPOSFI",
            NA_PATH="/sp/3550308/77",
            NA_FILE="sample_DONE.mat",
            NA_EXTENSION=".mat",
            VL_FILE_SIZE_KB=10,
        )

        self.assertEqual(file_id, 701)
        self.assertEqual(inserted["table"], "DIM_SPECTRUM_FILE")
        self.assertEqual(inserted["data"]["NA_VOLUME"], "reposfi")
        self.assertEqual(inserted["data"]["ID_TYPE_FILE"], 41)


class ProcedureAndEquipmentTests(DbHandlerRfmBaseTests):
    """Validate small dimensions reused by spectrum ingestion."""

    def test_insert_procedure_reuses_existing_row(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [{"ID_PROCEDURE": 91}]

        procedure_id = handler.insert_procedure("Drive test")

        self.assertEqual(procedure_id, 91)

    def test_get_equipment_types_normalizes_uids(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {"ID_EQUIPMENT_TYPE": 1, "NA_EQUIPMENT_TYPE_UID": " RFeye "},
            {"ID_EQUIPMENT_TYPE": 2, "NA_EQUIPMENT_TYPE_UID": "Keysight"},
        ]

        types_map = handler._get_equipment_types()

        self.assertEqual(types_map["rfeye"]["id"], 1)
        self.assertEqual(types_map["keysight"]["id"], 2)

    def test_get_or_create_spectrum_equipment_inserts_when_missing(self) -> None:
        handler = self.make_handler()
        inserted = {}
        handler._get_equipment_types = lambda: {"keysight": {"id": 8}}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            inserted["table"] = table
            inserted["data"] = data
            return 501

        handler._insert_row = fake_insert_row

        equipment_id = handler.get_or_create_spectrum_equipment(
            "Keysight Technologies,N9936B,MY59221878,A.11.55"
        )

        self.assertEqual(equipment_id, 501)
        self.assertEqual(inserted["table"], "DIM_SPECTRUM_EQUIPMENT")
        self.assertEqual(inserted["data"]["FK_EQUIPMENT_TYPE"], 8)
        self.assertEqual(
            inserted["data"]["NA_EQUIPMENT"],
            "keysight technologies,n9936b,my59221878,a.11.55",
        )

    def test_get_or_create_spectrum_equipment_rejects_unknown_receiver(self) -> None:
        handler = self.make_handler()
        handler._get_equipment_types = lambda: {"rfeye": {"id": 1}}

        with self.assertRaises(Exception) as ctx:
            handler.get_or_create_spectrum_equipment("mystery receiver")

        self.assertIn("Unable to infer equipment type", str(ctx.exception))

    def test_get_or_create_spectrum_equipment_uses_explicit_type_hint(self) -> None:
        handler = self.make_handler()
        inserted = {}
        handler._get_equipment_types = lambda: {"sa2500": {"id": 8}}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            inserted["table"] = table
            inserted["data"] = data
            return 777

        handler._insert_row = fake_insert_row

        equipment_id = handler.get_or_create_spectrum_equipment(
            "ERMxES03",
            equipment_type_hint="TEKTRONIX,SA2500,B040241,7.041",
        )

        self.assertEqual(equipment_id, 777)
        self.assertEqual(inserted["table"], "DIM_SPECTRUM_EQUIPMENT")
        self.assertEqual(inserted["data"]["FK_EQUIPMENT_TYPE"], 8)
        self.assertEqual(inserted["data"]["NA_EQUIPMENT"], "ermxes03")


class SmallDimensionInsertTests(DbHandlerRfmBaseTests):
    """Validate small idempotent dimensions used by FACT_SPECTRUM."""

    def test_insert_detector_type_reuses_existing_row(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [{"ID_DETECTOR": 61}]

        detector_id = handler.insert_detector_type("Peak")

        self.assertEqual(detector_id, 61)

    def test_insert_measure_unit_inserts_new_row(self) -> None:
        handler = self.make_handler()
        captured = {}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            captured["table"] = table
            captured["data"] = data
            return 71

        handler._insert_row = fake_insert_row

        unit_id = handler.insert_measure_unit("dBm")

        self.assertEqual(unit_id, 71)
        self.assertEqual(captured["table"], "DIM_SPECTRUM_UNIT")
        self.assertEqual(captured["data"]["NA_MEASURE_UNIT"], "dBm")

    def test_insert_trace_type_inserts_new_row(self) -> None:
        handler = self.make_handler()
        captured = {}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            captured["table"] = table
            captured["data"] = data
            return 81

        handler._insert_row = fake_insert_row

        trace_id = handler.insert_trace_type("Average")

        self.assertEqual(trace_id, 81)
        self.assertEqual(captured["table"], "DIM_SPECTRUM_TRACE_TYPE")
        self.assertEqual(captured["data"]["NA_TRACE_TYPE"], "Average")


class SpectrumAndBridgeTests(DbHandlerRfmBaseTests):
    """Validate the final relational inserts that tie the ingestion together."""

    def test_insert_spectrum_serializes_js_metadata_dict(self) -> None:
        handler = self.make_handler()
        captured = {}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            captured["table"] = table
            captured["data"] = data
            return 801

        handler._insert_row = fake_insert_row

        spectrum_id = handler.insert_spectrum(
            {
                "id_site": 10,
                "id_equipment": 20,
                "id_procedure": 30,
                "id_detector_type": 40,
                "id_trace_type": 50,
                "id_measure_unit": 60,
                "nu_freq_start": 70.0,
                "nu_freq_end": 80.0,
                "dt_time_start": datetime(2026, 1, 1, 10, 0, 0),
                "dt_time_end": datetime(2026, 1, 1, 10, 1, 0),
                "nu_trace_length": 1281,
                "js_metadata": {"antenna": {"Name": "ANT-01"}},
            }
        )

        self.assertEqual(spectrum_id, 801)
        self.assertEqual(captured["table"], "FACT_SPECTRUM")
        self.assertEqual(
            captured["data"]["JS_METADATA"],
            '{"antenna": {"Name": "ANT-01"}}',
        )

    def test_insert_bridge_spectrum_file_emits_all_pairs(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor()

        handler.insert_bridge_spectrum_file([1, 2], [10, 11])

        self.assertEqual(len(handler.cursor.executed), 1)
        sql, params = handler.cursor.executed[0]
        self.assertIn("INSERT IGNORE INTO BRIDGE_SPECTRUM_FILE", sql)
        self.assertEqual(params, [(1, 10), (1, 11), (2, 10), (2, 11)])
        self.assertEqual(handler.db_connection.commits, 1)


class PublicationTests(DbHandlerRfmBaseTests):
    """Validate helpers used by metadata publication and export."""

    def test_export_parquet_writes_one_file_per_table(self) -> None:
        handler = self.make_handler()
        handler.cursor = FakeCursor(
            fetch_batches=[
                [("FACT_SPECTRUM",), ("DIM_SPECTRUM_FILE",)],
                [(1, "desc")],
                [("ID_SPECTRUM",), ("NA_DESCRIPTION",)],
                [(10, "sample.mat")],
                [("ID_FILE",), ("NA_FILE",)],
            ]
        )
        written = []

        def fake_to_parquet(self, file_name):
            written.append(file_name)

        with patch.object(db_rfm_module.pd.DataFrame, "to_parquet", fake_to_parquet):
            handler.export_parquet("/tmp/rfdata_snapshot")

        self.assertEqual(
            written,
            [
                "/tmp/rfdata_snapshot.FACT_SPECTRUM.parquet",
                "/tmp/rfdata_snapshot.DIM_SPECTRUM_FILE.parquet",
            ],
        )

    def test_get_latest_processing_time_returns_unix_timestamp(self) -> None:
        handler = self.make_handler()
        latest = datetime(2026, 3, 26, 12, 30, 0)
        handler._select_rows = lambda **kwargs: [{"LATEST": latest}]

        result = handler.get_latest_processing_time()

        self.assertEqual(result, latest.timestamp())


if __name__ == "__main__":
    unittest.main()
