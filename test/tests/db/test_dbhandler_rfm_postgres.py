"""Exercise the migrated RFDATA dimension operations through Psycopg."""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import sys
import unittest

import psycopg
from psycopg._queries import PostgresQuery
from psycopg.adapt import Transformer
from psycopg.pq import TransactionStatus

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, load_test_config

sys.path.insert(0, str(APP_ROOT))
with patch.dict(sys.modules, {"config": load_test_config()}):
    from db.dbHandlerRFM import dbHandlerRFM


class PostgresRfmTests(unittest.TestCase):
    """Verify binding, returning and duplicate-safe bridge insertion."""

    def setUp(self) -> None:
        """Configure a disconnected handler and a driver-shaped session.

        Args:
            None.
        Returns:
            None.
        """
        self.handler = dbHandlerRFM("RFDATA_TEST", MagicMock(), conninfo="dbname=test")
        self.connection = MagicMock(spec=psycopg.Connection)
        self.connection.closed = False
        self.connection.broken = False
        self.connection.info = SimpleNamespace(
            transaction_status=TransactionStatus.IDLE
        )
        self.cursor = MagicMock(spec=psycopg.Cursor)
        self.cursor.connection = self.connection
        self.connection.cursor.return_value.__enter__.return_value = self.cursor
        self.cursor.rowcount = 1
        self.connect = patch.object(psycopg.Connection, "connect", return_value=self.connection)
        self.connect.start()
        self.addCleanup(self.connect.stop)
        self.addCleanup(self.handler.close)

    def test_dimension_reuses_existing_or_returns_inserted_identity(self) -> None:
        """Keep hostile labels as values and retrieve IDs with RETURNING.

        Args:
            None.
        Returns:
            None.
        """
        hostile = "trace'; DROP TABLE FACT_SPECTRUM; --"
        self.cursor.fetchall.return_value = [{"ID_TRACE_TYPE": 8}]
        self.assertEqual(self.handler.insert_trace_type(hostile), 8)
        query, params = self.cursor.execute.call_args.args
        self.assertNotIn(hostile, query.as_string())
        self.assertEqual(params, (hostile, 1))
        PostgresQuery(Transformer()).convert(query, params)
        self.cursor.fetchall.return_value = []
        self.cursor.fetchone.return_value = {"ID_TRACE_TYPE": 9}
        self.assertEqual(self.handler.insert_trace_type(hostile), 9)
        query, params = self.cursor.execute.call_args.args
        self.assertIn('RETURNING "ID_TRACE_TYPE"', query.as_string())
        self.assertEqual(params, (hostile,))
        PostgresQuery(Transformer()).convert(query, params)

    def test_bridge_uses_bound_pairs_and_skips_existing_links(self) -> None:
        """Insert the Cartesian product without changing the caller transaction.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.insert_bridge_spectrum_file([1, 2], [3, 4])
        query, values = self.cursor.executemany.call_args.args
        self.assertIn("ON CONFLICT DO NOTHING", query.as_string())
        self.assertEqual(values, [(1, 3), (1, 4), (2, 3), (2, 4)])
        for row in values:
            PostgresQuery(Transformer()).convert(query, row)
        self.cursor.executemany.reset_mock()
        self.handler.insert_bridge_spectrum_file([], [3])
        self.cursor.executemany.assert_not_called()

    def test_location_coordinates_are_values_and_threshold_is_respected(self) -> None:
        """Bind point coordinates separately and reject distant matches.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [{"IDENTITY": 7, "DISTANCE": 1.5}]
        self.assertEqual(
            self.handler.get_site_id({"longitude": -46.5, "latitude": -23.2}), 7
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(params, (-46.5, -23.2, 1))
        self.assertNotIn("-46.5", query.as_string())
        PostgresQuery(Transformer()).convert(query, params)
        for distance in (70.0, 1000.0):
            with self.subTest(distance=distance):
                self.cursor.fetchall.return_value = [{"IDENTITY": 7, "DISTANCE": distance}]
                self.assertFalse(
                    self.handler.get_site_id({"longitude": -46.5, "latitude": -23.2})
                )
        self.cursor.fetchall.return_value = []
        self.assertIs(
            self.handler.get_site_id({"longitude": -46.5, "latitude": -23.2}), False
        )

    def test_geographic_candidates_are_normalized_and_deduplicated(self) -> None:
        """Match accented locality variants without losing candidate order.

        Args:
            None.
        Returns:
            None.
        """
        data = {"district": "  Sé  ", "district_candidates": ["Se", " Vila ", "vila"]}
        self.assertEqual(
            self.handler._iter_normalized_field_candidates(data, "district"),
            [("Sé", "se"), ("Vila", "vila")],
        )

    def test_site_insert_binds_geometry_and_uses_returning(self) -> None:
        """Keep site coordinates outside SQL and obtain the generated identity.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchone.return_value = {"ID_SITE": 12}
        payload = {
            "longitude": -46.5,
            "latitude": -23.2,
            "altitude": 10,
            "county": "Example",
        }
        with patch.object(
            self.handler, "_get_geographic_codes", return_value=(1, 2, None)
        ), patch.object(self.handler, "_summary_publish_scope"):
            self.assertEqual(self.handler.insert_site(payload), 12)
        query, params = self.cursor.execute.call_args.args
        self.assertNotIn("-46.5", query.as_string())
        self.assertIn("POINT(-46.5 -23.2)", params)
        self.assertIn('RETURNING "ID_SITE"', query.as_string())
        PostgresQuery(Transformer()).convert(query, params)


    def _calls(self) -> list[tuple]:
        """Return executed (SQL text, params) pairs, skipping the savepoint opener.

        Args:
            None.
        Returns:
            list[tuple]: Rendered statement and bound parameters, in order.
        """
        return [
            (call.args[0].as_string(), call.args[1] if len(call.args) > 1 else ())
            for call in self.cursor.execute.call_args_list
            if call.args[0].as_string() != "SELECT 1"
        ]

    def _statements(self) -> list[str]:
        """Return the SQL text of every executed domain statement in order.

        Args:
            None.
        Returns:
            list[str]: Rendered statements.
        """
        return [text for text, _ in self._calls()]

    def test_get_or_insert_rereads_after_losing_insert_race(self) -> None:
        """Return the concurrent winner instead of creating a duplicate label.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [[], [{"ID_PROCEDURE": 5}]]
        self.cursor.fetchone.return_value = None
        self.assertEqual(self.handler.insert_procedure("PROC"), 5)
        select, insert, reread = self._statements()
        self.assertIn("ON CONFLICT DO NOTHING", insert)
        self.assertEqual(select, reread)

    def test_insert_file_refreshes_only_supplied_fields(self) -> None:
        """Keep stored metadata when the caller omits it, inside one transaction.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [{"ID_FILE": 7}]
        with patch.object(self.handler, "get_file_type_id_by_hostname", return_value=3):
            file_id = self.handler.insert_file(
                "rfeye002090", "/data", "a.bin", "VOL", NA_EXTENSION=".bin"
            )
        self.assertEqual(file_id, 7)
        query, params = self.cursor.execute.call_args.args
        self.assertTrue(query.as_string().startswith('UPDATE "RFDATA_TEST"."DIM_SPECTRUM_FILE"'))
        self.assertEqual(params, (3, ".bin", 7))
        select_params = self.cursor.execute.call_args_list[0].args[1]
        self.assertEqual(select_params[:3], ("vol", "/data", "a.bin"))
        self.connection.commit.assert_called_once_with()

    def _spectrum(self, end: datetime) -> dict:
        """Build one resolved spectrum payload.

        Args:
            end (datetime): Interval end.
        Returns:
            dict: insert_spectrum input.
        """
        return {
            "id_site": 1, "id_equipment": 2, "id_procedure": 3,
            "id_detector_type": 4, "id_trace_type": 5, "id_measure_unit": 6,
            "nu_freq_start": 70.0, "nu_freq_end": 110.0,
            "dt_time_start": datetime(2025, 1, 1), "dt_time_end": end,
            "nu_trace_length": 1024, "nu_trace_count": 10,
        }

    def test_spectrum_locks_site_then_reuses_or_widens(self) -> None:
        """Serialize writers per site and keep one row per logical spectrum.

        Args:
            None.
        Returns:
            None.
        """
        stored = {"ID_SPECTRUM": 4, "DT_TIME_END": datetime(2025, 1, 2), "NU_TRACE_COUNT": 5}
        self.cursor.fetchall.side_effect = [[{"ID_SITE": 1}], [stored]]
        self.assertEqual(self.handler.insert_spectrum(self._spectrum(datetime(2025, 1, 3))), 4)
        lock, lookup, update = self._statements()
        self.assertTrue(lock.endswith("FOR UPDATE"))
        self.assertIn('"DIM_SPECTRUM_SITE"', lock)
        self.assertTrue(update.startswith('UPDATE "RFDATA_TEST"."FACT_SPECTRUM"'))
        self.assertEqual(self._calls()[-1][1][-1], 4)
        self.cursor.execute.reset_mock()
        self.cursor.fetchall.side_effect = [[{"ID_SITE": 1}], [stored]]
        self.assertEqual(self.handler.insert_spectrum(self._spectrum(datetime(2025, 1, 1, 12))), 4)
        self.assertEqual(len(self._statements()), 2)

    def test_file_ids_match_exact_triples_from_one_query(self) -> None:
        """Discard cross-product rows returned by the member-set filter.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"ID_FILE": 1, "NA_VOLUME": "v", "NA_PATH": "/a", "NA_FILE": "x"},
            {"ID_FILE": 2, "NA_VOLUME": "v", "NA_PATH": "/a", "NA_FILE": "y"},
            {"ID_FILE": 3, "NA_VOLUME": "v", "NA_PATH": "/b", "NA_FILE": "y"},
        ]
        ids = self.handler._select_file_ids_by_artifacts(
            artifacts=[("V", "/a", "x"), ("v", "/b", "y")]
        )
        self.assertEqual(ids, [1, 3])
        self.assertEqual(self.cursor.execute.call_count, 1)

    def test_lineage_prunes_only_spectra_without_remaining_links(self) -> None:
        """Delete a spectrum only after its last file link disappears.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [
            [{"FK_SPECTRUM": 2}],
            [{"ID_SPECTRUM": 1, "FK_SITE": 9, "FK_EQUIPMENT": None}],
        ]
        result = self.handler._remove_lineage(
            file_ids=[7], spectrum_ids=[1, 2], reason="test"
        )
        self.assertEqual(result["removed_spectra"], 1)
        statements = self._statements()
        self.assertTrue(statements[0].startswith('DELETE FROM "RFDATA_TEST"."BRIDGE_SPECTRUM_FILE"'))
        self.assertEqual(self.cursor.execute.call_args_list[2].args[1], (1,))
        self.assertTrue(statements[-1].startswith('DELETE FROM "RFDATA_TEST"."FACT_SPECTRUM"'))
        self.assertEqual(self._calls()[-1][1], (1,))
        self.assertEqual(self.cursor.executemany.call_args.args[1][0][:2], ("site", "9"))

    def test_update_site_locks_once_and_tolerates_missing_altitude(self) -> None:
        """Weight stored coordinates and use only samples for an unset altitude.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"LONGITUDE": 10.0, "LATITUDE": 20.0, "NU_ALTITUDE": None,
             "NU_GNSS_MEASUREMENTS": 2, "SRID": 4326}
        ]
        result = self.handler.update_site(1, [13.0], [23.0], [100.0])
        self.assertEqual((result["longitude"], result["latitude"]), (11.0, 21.0))
        self.assertEqual(result["altitude"], 100.0)
        read, update = self._statements()
        self.assertTrue(read.endswith("FOR UPDATE"))
        self.assertIn("SRID=4326;POINT(11.0 21.0)", self._calls()[-1][1])

    def test_equipment_type_matches_normalized_uid_and_ignores_blank(self) -> None:
        """Infer the type from alphanumeric tokens and publish new equipment once.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [
            [
                {"ID_EQUIPMENT_TYPE": 3, "NA_EQUIPMENT_TYPE_UID": "  "},
                {"ID_EQUIPMENT_TYPE": 2, "NA_EQUIPMENT_TYPE_UID": "RFeye-Node"},
            ],
            [],
        ]
        self.cursor.fetchone.return_value = {"ID_EQUIPMENT": 11}
        equipment_id = self.handler.get_or_create_spectrum_equipment(
            "RFEYE002090", equipment_type_hint="RFeye Node 100"
        )
        self.assertEqual(equipment_id, 11)
        self.assertEqual(self._calls()[-1][1], ("rfeye002090", 2))
        self.cursor.executemany.assert_called_once()



if __name__ == "__main__":
    unittest.main()
