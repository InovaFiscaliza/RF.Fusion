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
    - bridge inserts
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
        handler.database = "RFDATA_TEST"
        handler.db_connection = FakeConnection()
        handler.cursor = FakeCursor()
        handler.in_transaction = False
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_scope = lambda *args, **kwargs: None
        return handler


class SqlBuilderTests(DbHandlerRfmBaseTests):
    """Validate shared WHERE-clause behavior inherited from `DBHandlerBase`."""

    def test_build_where_clause_uses_is_null_for_none_equality(self) -> None:
        handler = self.make_handler()
        params: list[object] = []

        clause = handler._build_where_clause(
            {"FK_SITE": 77, "NU_VBW": None, "NA_DESCRIPTION": "PMEC"},
            params,
        )

        self.assertEqual(
            clause,
            " WHERE FK_SITE=%s AND NU_VBW IS NULL AND NA_DESCRIPTION=%s",
        )
        self.assertEqual(params, [77, "PMEC"])


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

    def test_insert_file_refreshes_existing_artifact_metadata(self) -> None:
        handler = self.make_handler()
        updated = {}
        created = datetime(2026, 4, 2, 13, 8, 56)
        modified = datetime(2026, 7, 8, 10, 15, 0)
        handler.get_file_type_id_by_hostname = lambda HOSTNAME: 41

        def fake_select_rows(*, table, where=None, cols=None, limit=None):
            if table == "DIM_SPECTRUM_FILE":
                return [{"ID_FILE": 700}]
            raise AssertionError(f"Unexpected select_rows call: {table=} {where=}")

        def fake_update_row(*, table, data, where, commit=True, **kwargs):
            updated["table"] = table
            updated["data"] = data
            updated["where"] = where
            updated["commit"] = commit
            return 1

        handler._select_rows = fake_select_rows
        handler._update_row = fake_update_row
        handler._insert_row = lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("insert should not be called for existing file")
        )

        file_id = handler.insert_file(
            hostname="CWSM21100001",
            NA_VOLUME="REPOSFI",
            NA_PATH="/sp/3550308/77",
            NA_FILE="sample_DONE.mat",
            NA_EXTENSION=".mat",
            VL_FILE_SIZE_KB=27352,
            DT_FILE_CREATED=created,
            DT_FILE_MODIFIED=modified,
        )

        self.assertEqual(file_id, 700)
        self.assertEqual(updated["table"], "DIM_SPECTRUM_FILE")
        self.assertEqual(updated["where"], {"ID_FILE": 700})
        self.assertEqual(
            updated["data"],
            {
                "ID_TYPE_FILE": 41,
                "NA_EXTENSION": ".mat",
                "VL_FILE_SIZE_KB": 27352,
                "DT_FILE_CREATED": created,
                "DT_FILE_MODIFIED": modified,
            },
        )
        self.assertTrue(updated["commit"])

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
        handler._get_equipment_types = lambda: {"ermx": {"id": 12}}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            inserted["table"] = table
            inserted["data"] = data
            return 777

        handler._insert_row = fake_insert_row

        equipment_id = handler.get_or_create_spectrum_equipment(
            "ERMxES03",
            equipment_type_hint="ermx",
        )

        self.assertEqual(equipment_id, 777)
        self.assertEqual(inserted["table"], "DIM_SPECTRUM_EQUIPMENT")
        self.assertEqual(inserted["data"]["FK_EQUIPMENT_TYPE"], 12)
        self.assertEqual(inserted["data"]["NA_EQUIPMENT"], "ermxes03")

    def test_get_or_create_spectrum_equipment_matches_ermx_station_type(self) -> None:
        handler = self.make_handler()
        inserted = {}
        handler._get_equipment_types = lambda: {"ermx": {"id": 12}}
        handler._select_rows = lambda **kwargs: []

        def fake_insert_row(*, table, data):
            inserted["table"] = table
            inserted["data"] = data
            return 778

        handler._insert_row = fake_insert_row

        equipment_id = handler.get_or_create_spectrum_equipment(
            "ermxgo01",
            equipment_type_hint="ermx",
        )

        self.assertEqual(equipment_id, 778)
        self.assertEqual(inserted["table"], "DIM_SPECTRUM_EQUIPMENT")
        self.assertEqual(inserted["data"]["FK_EQUIPMENT_TYPE"], 12)
        self.assertEqual(inserted["data"]["NA_EQUIPMENT"], "ermxgo01")


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

    def test_insert_spectrum_updates_existing_row_when_incoming_interval_is_broader(self) -> None:
        handler = self.make_handler()
        updated = {}
        handler._select_rows = lambda **kwargs: [
            {
                "ID_SPECTRUM": 4223243,
                "DT_TIME_START": datetime(2026, 5, 18, 7, 55, 0),
                "DT_TIME_END": datetime(2026, 5, 18, 21, 50, 0),
                "NU_TRACE_COUNT": 168,
            }
        ]
        handler._insert_row = lambda **kwargs: self.fail(
            "should not insert a new spectrum"
        )

        def fake_update_row(*, table, data, where, commit=True, **kwargs):
            updated["table"] = table
            updated["data"] = data
            updated["where"] = where
            updated["commit"] = commit
            return 1

        handler._update_row = fake_update_row

        spectrum_id = handler.insert_spectrum(
            {
                "id_site": 10,
                "id_equipment": 20,
                "id_procedure": 30,
                "id_detector_type": 40,
                "id_trace_type": 50,
                "id_measure_unit": 60,
                "nu_freq_start": 470.0,
                "nu_freq_end": 700.0,
                "dt_time_start": datetime(2026, 5, 18, 7, 55, 0),
                "dt_time_end": datetime(2026, 5, 19, 12, 0, 0),
                "nu_trace_count": 338,
                "nu_trace_length": 5888,
            }
        )

        self.assertEqual(spectrum_id, 4223243)
        self.assertEqual(updated["table"], "FACT_SPECTRUM")
        self.assertEqual(updated["where"], {"ID_SPECTRUM": 4223243})
        self.assertEqual(updated["data"]["DT_TIME_END"], datetime(2026, 5, 19, 12, 0, 0))
        self.assertEqual(updated["data"]["NU_TRACE_COUNT"], 338)
        self.assertTrue(updated["commit"])

    def test_insert_spectrum_reuses_existing_row_when_incoming_interval_is_contained(self) -> None:
        handler = self.make_handler()
        handler._select_rows = lambda **kwargs: [
            {
                "ID_SPECTRUM": 900,
                "DT_TIME_START": datetime(2026, 5, 18, 7, 55, 0),
                "DT_TIME_END": datetime(2026, 5, 19, 12, 0, 0),
                "NU_TRACE_COUNT": 338,
            }
        ]
        handler._insert_row = lambda **kwargs: self.fail(
            "should not insert a new spectrum"
        )
        handler._update_row = lambda **kwargs: self.fail(
            "should not update a broader existing spectrum"
        )

        spectrum_id = handler.insert_spectrum(
            {
                "id_site": 10,
                "id_equipment": 20,
                "id_procedure": 30,
                "id_detector_type": 40,
                "id_trace_type": 50,
                "id_measure_unit": 60,
                "nu_freq_start": 470.0,
                "nu_freq_end": 700.0,
                "dt_time_start": datetime(2026, 5, 18, 7, 55, 0),
                "dt_time_end": datetime(2026, 5, 18, 21, 50, 0),
                "nu_trace_count": 168,
                "nu_trace_length": 5888,
            }
        )

        self.assertEqual(spectrum_id, 900)

    def test_insert_spectrum_keeps_nullable_identity_fields_in_lookup(self) -> None:
        handler = self.make_handler()
        select_calls = []

        def fake_select_rows(**kwargs):
            select_calls.append(kwargs)
            return [
                {
                    "ID_SPECTRUM": 901,
                    "DT_TIME_START": datetime(2026, 6, 17, 17, 43, 5),
                    "DT_TIME_END": datetime(2026, 7, 1, 22, 30, 55),
                    "NU_TRACE_COUNT": 4090,
                }
            ]

        handler._select_rows = fake_select_rows
        handler._insert_row = lambda **kwargs: self.fail(
            "should not insert a new spectrum"
        )
        handler._update_row = lambda **kwargs: self.fail(
            "should not update an already matching spectrum"
        )

        spectrum_id = handler.insert_spectrum(
            {
                "id_site": 396,
                "id_equipment": 150,
                "id_procedure": 53,
                "id_detector_type": 1,
                "id_trace_type": 6,
                "id_measure_unit": 9,
                "na_description": "PMEC (Faixa 7 de 16) (SMP)",
                "nu_freq_start": 703.0,
                "nu_freq_end": 960.0,
                "dt_time_start": datetime(2026, 6, 17, 17, 43, 5),
                "dt_time_end": datetime(2026, 7, 1, 22, 30, 55),
                "nu_trace_count": 4090,
                "nu_trace_length": 5141,
                "nu_rbw": 120000.0,
                "nu_vbw": None,
                "nu_att_gain": 0.0,
            }
        )

        self.assertEqual(spectrum_id, 901)
        self.assertEqual(len(select_calls), 1)
        self.assertEqual(
            select_calls[0]["where"]["NU_VBW"],
            None,
        )

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

    def test_reconcile_reprocessed_file_lineage_prunes_shorter_duplicate(self) -> None:
        handler = self.make_handler()
        summary_calls = []
        handler._summary_publish_scope = lambda **kwargs: summary_calls.append(kwargs)
        handler._select_file_ids_by_artifacts = lambda **kwargs: [501, 502]
        handler._select_spectrum_merge_rows_by_file_ids = lambda **kwargs: [
            {
                "ID_SPECTRUM": 1001,
                "FK_SITE": 7,
                "FK_DETECTOR": 4,
                "FK_TRACE_TYPE": 5,
                "FK_MEASURE_UNIT": 6,
                "FK_PROCEDURE": 3,
                "FK_EQUIPMENT": 8,
                "NA_DESCRIPTION": "Undefined",
                "NU_FREQ_START": 53.910156,
                "NU_FREQ_END": 87.992188,
                "DT_TIME_START": datetime(2025, 10, 31, 15, 50, 27),
                "DT_TIME_END": datetime(2025, 11, 1, 3, 49, 51),
                "NU_TRACE_COUNT": 720,
                "NU_TRACE_LENGTH": 350,
                "NU_RBW": 97656.3,
                "NU_VBW": None,
                "NU_ATT_GAIN": -10.0,
            },
            {
                "ID_SPECTRUM": 1002,
                "FK_SITE": 7,
                "FK_DETECTOR": 4,
                "FK_TRACE_TYPE": 5,
                "FK_MEASURE_UNIT": 6,
                "FK_PROCEDURE": 3,
                "FK_EQUIPMENT": 8,
                "NA_DESCRIPTION": "Undefined",
                "NU_FREQ_START": 53.910156,
                "NU_FREQ_END": 87.992188,
                "DT_TIME_START": datetime(2025, 10, 31, 15, 50, 27),
                "DT_TIME_END": datetime(2025, 11, 1, 3, 48, 51),
                "NU_TRACE_COUNT": 719,
                "NU_TRACE_LENGTH": 350,
                "NU_RBW": 97656.3,
                "NU_VBW": None,
                "NU_ATT_GAIN": -10.0,
            },
        ]
        deleted_pairs = {}

        def fake_delete_bridge_file_links(*, file_ids, spectrum_ids):
            deleted_pairs["file_ids"] = file_ids
            deleted_pairs["spectrum_ids"] = spectrum_ids
            return 2

        handler._delete_bridge_file_links = fake_delete_bridge_file_links
        handler._select_orphan_spectrum_context = lambda **kwargs: [
            {"ID_SPECTRUM": 1002, "FK_SITE": 7, "FK_EQUIPMENT": 8}
        ]

        deleted_rows = []

        def fake_delete_rows_by_int_ids(*, table, column, ids):
            deleted_rows.append((table, column, ids))
            return 1

        handler._delete_rows_by_int_ids = fake_delete_rows_by_int_ids

        result = handler.reconcile_reprocessed_file_lineage(
            host_volume="cwsm21100011",
            host_path="C:/host",
            host_file="source.zip",
            repository_volume="reposfi",
            repository_path="/mnt/reposfi/2025/site_219/catalog",
            repository_file="sample_DONE.mat",
        )

        self.assertEqual(
            result,
            {
                "file_ids": 2,
                "removed_file_links": 2,
                "removed_emitter_links": 1,
                "removed_spectra": 1,
            },
        )
        self.assertEqual(deleted_pairs["file_ids"], [501, 502])
        self.assertEqual(deleted_pairs["spectrum_ids"], [1002])
        self.assertEqual(
            deleted_rows,
            [
                ("BRIDGE_SPECTRUM_EMITTER", "FK_SPECTRUM", [1002]),
                ("FACT_SPECTRUM", "ID_SPECTRUM", [1002]),
            ],
        )
        self.assertEqual(
            summary_calls[0]["reason"],
            "reconcile_reprocessed_file_lineage",
        )

    def test_reset_reprocessed_file_lineage_replaces_previous_file_pair(self) -> None:
        handler = self.make_handler()
        summary_calls = []
        handler._summary_publish_scope = lambda **kwargs: summary_calls.append(kwargs)
        handler._select_file_ids_by_artifacts = lambda **kwargs: [601, 602]
        handler._select_spectrum_merge_rows_by_file_ids = lambda **kwargs: [
            {"ID_SPECTRUM": 2001, "FK_SITE": 7, "FK_EQUIPMENT": 8},
            {"ID_SPECTRUM": 2002, "FK_SITE": 7, "FK_EQUIPMENT": 8},
        ]
        deleted_file_ids = {}

        def fake_delete_bridge_file_links_by_file_ids(*, file_ids):
            deleted_file_ids["file_ids"] = file_ids
            return 4

        handler._delete_bridge_file_links_by_file_ids = (
            fake_delete_bridge_file_links_by_file_ids
        )
        handler._select_orphan_spectrum_context = lambda **kwargs: [
            {"ID_SPECTRUM": 2001, "FK_SITE": 7, "FK_EQUIPMENT": 8},
            {"ID_SPECTRUM": 2002, "FK_SITE": 7, "FK_EQUIPMENT": 8},
        ]

        deleted_rows = []

        def fake_delete_rows_by_int_ids(*, table, column, ids):
            deleted_rows.append((table, column, ids))
            return len(ids)

        handler._delete_rows_by_int_ids = fake_delete_rows_by_int_ids

        result = handler.reset_reprocessed_file_lineage(
            host_volume="cwsm212031",
            host_path="C:/host",
            host_file="source.zip",
            repository_volume="reposfi",
            repository_path="/mnt/reposfi/2025/site_80/catalog",
            repository_file="sample_DONE.mat",
        )

        self.assertEqual(
            result,
            {
                "file_ids": 2,
                "removed_file_links": 4,
                "removed_emitter_links": 2,
                "removed_spectra": 2,
            },
        )
        self.assertEqual(deleted_file_ids["file_ids"], [601, 602])
        self.assertEqual(
            deleted_rows,
            [
                ("BRIDGE_SPECTRUM_EMITTER", "FK_SPECTRUM", [2001, 2002]),
                ("FACT_SPECTRUM", "ID_SPECTRUM", [2001, 2002]),
            ],
        )
        self.assertEqual(
            summary_calls[0]["reason"],
            "reset_reprocessed_file_lineage",
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


if __name__ == "__main__":
    unittest.main()
