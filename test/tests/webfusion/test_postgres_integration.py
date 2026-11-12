"""Validate migrated WebFusion SQL and public payloads in a disposable database."""

from __future__ import annotations

import importlib
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

ROOT = Path(__file__).resolve().parents[3]


class WebFusionPostgresIntegrationTests(unittest.TestCase):
    """Exercise real SQL without writing to any shared application database."""

    @classmethod
    def setUpClass(cls) -> None:
        """Create the four schemas only in an explicitly disposable database.

        Args:
            None.
        Returns:
            None.
        Raises:
            unittest.SkipTest: If no integration connection was provided.
            ValueError: If the database name is not prefixed rffusion_test_.
        """
        cls.conninfo = os.environ.get('RFFUSION_TEST_PG_CONNINFO', '')
        if not cls.conninfo:
            raise unittest.SkipTest('RFFUSION_TEST_PG_CONNINFO is not configured.')
        if not conninfo_to_dict(cls.conninfo).get('dbname', '').startswith('rffusion_test_'):
            raise ValueError('WebFusion integration requires a disposable rffusion_test_* database.')
        sys.path.insert(0, str(ROOT / 'src/webfusion'))
        with psycopg.connect(cls.conninfo, autocommit=True) as connection:
            for filename in ('createProcessingDB.sql', 'createMeasureDB.sql',
                             'createFusionSummaryDB.sql', 'createWebFusionDB.sql'):
                ddl = (ROOT / 'src/postgres/scripts' / filename).read_text()
                ddl = '\n'.join(line for line in ddl.splitlines() if not line.startswith('\\copy'))
                connection.execute(ddl)
        cls.db = importlib.import_module('db.connection')
        cls.setting = patch.object(cls.db, 'PG_CONNINFO', cls.conninfo)
        cls.setting.start()
        cls.addClassCleanup(cls.setting.stop)
        cls.application = importlib.import_module('app')
        cls.application.app.config['TESTING'] = True
        cls.application.app.logger.disabled = True

    def setUp(self) -> None:
        """Seed independent fixtures before each case.

        Args:
            None.
        Returns:
            None.
        """
        with psycopg.connect(self.conninfo, autocommit=True) as connection:
            tables = connection.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('BPDATA','RFDATA','RFFUSION_SUMMARY','WEBFUSION') AND table_type = 'BASE TABLE'").fetchall()
            connection.execute(sql.SQL('TRUNCATE {} RESTART IDENTITY CASCADE').format(
                sql.SQL(', ').join(sql.Identifier(schema, table) for schema, table in tables)))
            connection.execute('''
                INSERT INTO "BPDATA"."HOST" ("ID_HOST", "NA_HOST_NAME", "IS_OFFLINE", "IS_BUSY")
                VALUES (1, 'RFeye000001', 0, 0), (2, 'RFeye000002', 1, 0);
                INSERT INTO "RFDATA"."DIM_SITE_TYPE" ("ID_TYPE", "NA_TYPE") VALUES (1, 'Fixed'), (2, 'Mobile');
                INSERT INTO "RFDATA"."DIM_SITE_STATE" ("ID_STATE", "NA_STATE", "LC_STATE") VALUES (35, 'São Paulo', 'SP');
                INSERT INTO "RFDATA"."DIM_SITE_COUNTY" ("ID_COUNTY", "FK_STATE", "NA_COUNTY") VALUES (3550308, 35, 'São Paulo');
                INSERT INTO "RFDATA"."DIM_SITE_DISTRICT" ("ID_DISTRICT", "FK_COUNTY", "NA_DISTRICT") VALUES (1, 3550308, 'Centro');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE" ("ID_SITE", "FK_TYPE", "FK_STATE", "FK_COUNTY", "FK_DISTRICT", "NA_SITE", "GEO_POINT")
                VALUES (1, 1, 35, 3550308, 1, 'Centro', 'SRID=4326;POINT(-46.5 -23.2)');
                INSERT INTO "RFDATA"."DIM_EQUIPMENT_TYPE" ("ID_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE_UID") VALUES (1, 'RFeye', 'rfeye');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_EQUIPMENT" ("ID_EQUIPMENT", "FK_EQUIPMENT_TYPE", "NA_EQUIPMENT") VALUES (1, 1, 'rfeye000001');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_PROCEDURE" ("ID_PROCEDURE", "NA_PROCEDURE") VALUES (1, 'Fixed logger');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_DETECTOR" ("ID_DETECTOR", "NA_DETECTOR") VALUES (1, 'RMS');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_TRACE_TYPE" ("ID_TRACE_TYPE", "NA_TRACE_TYPE") VALUES (1, 'Average');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_UNIT" ("ID_MEASURE_UNIT", "NA_MEASURE_UNIT") VALUES (1, 'dBm');
                INSERT INTO "RFDATA"."DIM_FILE_TYPE" ("ID_TYPE_FILE", "NA_TYPE_FILE", "NA_EQUIPMENT") VALUES (1, 'BIN', 'rfeye');
                INSERT INTO "RFDATA"."DIM_SPECTRUM_FILE" ("ID_FILE", "ID_TYPE_FILE", "NA_VOLUME", "NA_PATH", "NA_FILE", "NA_EXTENSION", "VL_FILE_SIZE_KB")
                VALUES (1, 1, 'reposfi', '/catalog', 'first.bin', '.bin', 100), (2, 1, 'reposfi', '/catalog', 'second.bin', '.bin', 200);
                INSERT INTO "RFDATA"."FACT_SPECTRUM" ("ID_SPECTRUM", "FK_SITE", "FK_EQUIPMENT", "FK_PROCEDURE", "FK_DETECTOR", "FK_TRACE_TYPE", "FK_MEASURE_UNIT", "NA_DESCRIPTION", "DT_TIME_START", "DT_TIME_END", "NU_FREQ_START", "NU_FREQ_END", "NU_TRACE_LENGTH")
                VALUES (1, 1, 1, 1, 1, 1, 1, 'First band', '2026-01-01', '2026-01-02', 70, 80, 1024),
                       (2, 1, 1, 1, 1, 1, 1, 'Second band', '2026-01-02', '2026-01-03', 80, 90, 1024),
                       (3, 1, 1, 1, 1, 1, 1, 'Third band', '2026-01-03', '2026-01-04', 90, 100, 1024);
                INSERT INTO "RFDATA"."BRIDGE_SPECTRUM_FILE" ("FK_SPECTRUM", "FK_FILE") VALUES (1, 1), (2, 1), (3, 2);
                INSERT INTO "RFFUSION_SUMMARY"."SITE_EQUIPMENT_METRIC" ("FK_SITE", "FK_EQUIPMENT", "DT_FIRST_SEEN_AT", "DT_LAST_SEEN_AT", "NU_SPECTRUM_COUNT", "ID_LAST_SPECTRUM") VALUES (1, 1, '2026-01-01', '2026-01-04', 3, 3);
                INSERT INTO "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK" ("FK_HOST", "FK_EQUIPMENT", "NA_MATCH_TYPE", "VL_MATCH_CONFIDENCE", "IS_PRIMARY_LINK") VALUES (1, 1, 'exact', 1.0, 1);
                INSERT INTO "RFFUSION_SUMMARY"."HOST_LOCATION_VISIT" ("FK_HOST", "NU_VISIT", "FK_SITE", "DT_FIRST_SEEN_AT", "DT_LAST_SEEN_AT", "NU_SPECTRUM_COUNT") VALUES (1, 1, 1, '2026-01-01', '2026-01-04', 3);
                INSERT INTO "RFFUSION_SUMMARY"."HOST_METRIC" ("FK_HOST", "DT_REFERENCE_MONTH", "NU_BACKUP_DONE_THIS_MONTH", "VL_BACKUP_DONE_GB_THIS_MONTH")
                VALUES (1, (date_trunc('month', CURRENT_DATE) - INTERVAL '1 month')::date, 100, 5), (2, date_trunc('month', CURRENT_DATE)::date, 3, 1.25);
            ''')

    def test_routes_and_external_payload_contracts(self) -> None:
        """Check real HTTP handlers, aliases, units and appAnalise lookahead.

        Args:
            None.
        Returns:
            None.
        """
        paths = ['/server', '/api/server/overview', '/api/server/hosts',
                 '/api/server/processing-errors', '/api/server/backup-errors',
                 '/api/server/usage-metrics', '/', '/api/map/stations', '/spectrum',
                 '/api/spectrum/filters?equipment_id=1', '/api/spectrum/localities?equipment_id=1',
                 '/api/spectrum/file/1/spectra', '/host?host_id=1',
                 '/api/host/1/statistics', '/api/host/1/locations', '/api/host/1/activity',
                 '/api/host/1/processing-errors', '/api/host/1/backup-errors',
                 '/api/appanalise/stations/summary', '/api/appanalise/hosts/1/stats',
                 '/api/appanalise/equipments', '/api/appanalise/states',
                 '/api/appanalise/localities?equipmentId=1',
                 '/api/appanalise/files/1/spectra', '/api/appanalise/files/count']
        with self.application.app.test_client() as client:
            for path in paths:
                with self.subTest(path=path):
                    response = client.get(path)
                    self.assertEqual(response.status_code, 200, response.get_json(silent=True))
            response = client.get('/api/server/zabbix_metrics')
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload['host_total'], 2)
            self.assertEqual(payload['backup_done_this_month'], 3)
            self.assertEqual(payload['backup_done_bytes_this_month'], 1342177280)
            self.assertNotIn('backup_done_gb_this_month', payload)
            self.assertEqual(client.get('/api/host/1/zabbix_metrics').status_code, 200)
            station_columns = client.get('/api/appanalise/stations/summary').get_json()['columns']
            self.assertEqual(station_columns, ['FK_HOST', 'FK_SITE', 'NA_HOST_NAME',
                'NA_LOCALITY_LABEL', 'NA_SITE_LABEL', 'FK_COUNTY', 'FK_DISTRICT',
                'NA_COUNTY_NAME', 'NA_DISTRICT_NAME', 'ID_STATE', 'NA_STATE_NAME',
                'NA_STATE_CODE', 'VL_LATITUDE', 'VL_LONGITUDE', 'VL_ALTITUDE',
                'DT_FIRST_SEEN_AT', 'DT_LAST_SEEN_AT', 'NU_SPECTRUM_COUNT',
                'NU_MATCHED_EQUIPMENT_TOTAL', 'IS_CURRENT_LOCATION',
                'IS_OFFLINE_SNAPSHOT', 'VL_MAX_MATCH_CONFIDENCE', 'DT_REFRESHED_AT'])
            page = client.get('/api/appanalise/files?pageSize=1').get_json()
            self.assertEqual(page['columns'], ['ID_FILE', 'NA_PATH', 'NA_FILE', 'NA_EXTENSION',
                'VL_FILE_SIZE_KB', 'DT_TIME_START', 'DT_TIME_END', 'NU_FREQ_START', 'NU_FREQ_END',
                'NU_SPECTRA', 'LOCALITY_COUNT', 'LOCALITY_LABELS', 'EQUIPMENT_LABELS'])
            self.assertEqual([row['ID_FILE'] for row in page['rows']], [2, 1])
            self.assertEqual(page['pagination'], {'page': 1, 'page_size': 1, 'has_more': True})
            self.assertEqual(page['rows'][1]['DT_TIME_START'], '2026-01-01T00:00:00')
            self.assertEqual(page['rows'][1]['NU_SPECTRA'], 2)

    def test_locality_labels_and_multiple_equipment_locations(self) -> None:
        """Keep legacy labels, equipment ties and per-host aggregation semantics.

        Args:
            None.
        Returns:
            None.
        """
        with psycopg.connect(self.conninfo) as connection:
            connection.execute('''INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE"
                ("ID_SITE", "FK_TYPE", "FK_STATE", "FK_COUNTY", "FK_DISTRICT", "NA_SITE", "GEO_POINT")
                VALUES (2, 1, 35, 3550308, 1, 'São Paulo', 'SRID=4326;POINT(-46.6 -23.3)'),
                       (3, 1, 35, 3550308, 1, '', 'SRID=4326;POINT(-46.7 -23.4)')''')
            connection.execute('''INSERT INTO "RFDATA"."DIM_SPECTRUM_EQUIPMENT"
                ("ID_EQUIPMENT", "FK_EQUIPMENT_TYPE", "NA_EQUIPMENT") VALUES (2, 1, 'other configuration');
                INSERT INTO "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK"
                ("FK_HOST", "FK_EQUIPMENT", "NA_MATCH_TYPE", "VL_MATCH_CONFIDENCE", "IS_PRIMARY_LINK")
                VALUES (1, 2, 'exact', 1, 1);
                INSERT INTO "RFFUSION_SUMMARY"."SITE_EQUIPMENT_METRIC"
                ("FK_SITE", "FK_EQUIPMENT", "DT_FIRST_SEEN_AT", "DT_LAST_SEEN_AT", "NU_SPECTRUM_COUNT")
                VALUES (2, 1, '2026-01-01', '2026-01-04', 1),
                       (3, 2, '2026-01-01', '2026-01-03', 1)''')
        queries = importlib.import_module('db.summary')
        connection = self.db.get_connection_summary()
        try:
            rows = connection.execute(f'SELECT * FROM ({queries.host_locations_query()}) q ORDER BY "FK_SITE"').fetchall()
            self.assertEqual([row['IS_CURRENT_LOCATION'] for row in rows], [0, 1, 1])
            self.assertEqual([row['NA_LOCALITY_LABEL'] for row in rows],
                ['Centro · São Paulo/SP', 'São Paulo/SP', 'Centro · São Paulo/SP'])
            self.assertEqual(rows[2]['NA_SITE_LABEL'], 'Site 3')
            hosts = connection.execute(f'SELECT * FROM ({queries.host_operational_query()}) q ORDER BY "ID_HOST"').fetchall()
            self.assertEqual(len(hosts), 2)
            self.assertEqual(hosts[0]['NA_CURRENT_LOCALITY_LABEL'], 'São Paulo/SP')
        finally:
            connection.close()

    def test_users_and_roles_commit_and_revoke(self) -> None:
        """Exercise PostgreSQL identity UPSERT, profile update and role changes.

        Args:
            None.
        Returns:
            None.
        """
        users = importlib.import_module('db.users')
        users.create_webfusion_user(user_name='Test User', user_email='test@example.test',
            job_title='Analyst', department='Test', location='SP', is_admin=True, is_developer=True)
        self.assertEqual(users.get_access_role('test@example.test'), 'admin')
        users.update_profile_image_url('test@example.test', '/static/test.png')
        self.assertEqual(users.get_profile_image_url('test@example.test'), '/static/test.png')
        users.update_webfusion_user_privileges(user_email='test@example.test', is_admin=False, is_developer=True)
        self.assertEqual(users.get_access_role('test@example.test'), 'developer')
        self.assertTrue(users.list_user_filter_options()['users'])
        users.delete_webfusion_user(user_email='test@example.test')
        self.assertIsNone(users.get_access_role('test@example.test'))

    def test_task_creation_reuses_identity_and_keeps_json(self) -> None:
        """Use RETURNING and JSON adaptation while preserving singleton tasks.

        Args:
            None.
        Returns:
            None.
        """
        tasks = importlib.import_module('modules.tasks.station_service')
        connection = self.db.get_connection_bpdata()
        try:
            args = dict(db=connection, host_id=1, task_type=1, filter_dict={'mode': 'NONE'}, message='test')
            self.assertEqual(tasks.queue_host_task_safe(**args), 'created')
            self.assertEqual(tasks.queue_host_task_safe(**args), 'refreshed')
            row = connection.execute('SELECT "FILTER" FROM "HOST_TASK"').fetchone()
            self.assertEqual(row['FILTER'], {'mode': 'NONE'})
        finally:
            connection.close()

    def test_checkpoint_and_usage_are_atomic(self) -> None:
        """Read a synthetic NGINX log twice without counting the same bytes twice.

        Args:
            None.
        Returns:
            None.
        """
        usage = importlib.import_module('modules.server.usage_metrics')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'access.log'
            path.write_text('127.0.0.1 - - [01/Nov/2026:12:00:00 +0000] "GET /downloads/test.bin HTTP/1.1" 200 100 "-" "test"\n')
            with patch.object(usage, '_get_nginx_download_log_path', return_value=str(path)):
                # A failed checkpoint must roll back the counters written before it.
                with patch.object(usage, '_save_checkpoint_with_cursor', side_effect=RuntimeError('Checkpoint write failed')):
                    with self.assertRaisesRegex(RuntimeError, 'Checkpoint write failed'):
                        usage.sync_nginx_download_metrics()
                with psycopg.connect(self.conninfo) as connection:
                    self.assertEqual(connection.execute('SELECT COUNT(*) FROM "RFFUSION_SUMMARY"."WEBFUSION_USAGE_METRIC_MONTHLY" WHERE "NA_METRIC_NAME" = %s', ('nginx_download_count',)).fetchone()[0], 0)
                    self.assertEqual(connection.execute('SELECT COUNT(*) FROM "RFFUSION_SUMMARY"."WEBFUSION_USAGE_LOG_CHECKPOINT"').fetchone()[0], 0)
                usage.sync_nginx_download_metrics()
                first = usage.get_usage_metrics_snapshot()
                usage.sync_nginx_download_metrics()
                second = usage.get_usage_metrics_snapshot()
            self.assertEqual(first['totals']['nginx_download_count'], 1)
            self.assertEqual(first['totals'], second['totals'])

    def test_protected_task_pages_read_postgres(self) -> None:
        """Render queue and history readers without contacting external Zabbix.

        Args:
            None.
        Returns:
            None.
        """
        with patch.object(self.application.AUTH_SERVICE, 'is_restricted_endpoint', return_value=False):
            with self.application.app.test_client() as client:
                for path in ('/tasks/stations', '/tasks/files', '/tasks/history', '/tasks/create'):
                    with self.subTest(path=path):
                        response = client.get(path)
                        self.assertEqual(response.status_code, 200)

    def test_host_stats_keep_history_scope_and_numeric_payload(self) -> None:
        """Preserve phase predicates, exact KB, empty hosts and numeric JSON types.

        Args:
            None.
        Returns:
            None.
        """
        with psycopg.connect(self.conninfo) as connection:
            connection.execute('INSERT INTO "BPDATA"."HOST" ("ID_HOST", "NA_HOST_NAME") VALUES (99, %s)', ('unrelated',))
            for index, (host, discovery, backup, size) in enumerate(((1, 0, 1, 1), (1, 0, 1, 2),
                    (1, -1, 1, 100), (1, 0, 0, 8), (1, 0, -1, 16), (99, 0, 1, 1000))):
                connection.execute('INSERT INTO "BPDATA"."FILE_TASK_HISTORY" ("FK_HOST", "NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME", "NU_STATUS_DISCOVERY", "NU_STATUS_BACKUP", "VL_FILE_SIZE_KB_HOST") VALUES (%s, %s, %s, %s, %s, %s)',
                    (host, '/input', f'{index}.bin', discovery, backup, size))
        with self.application.app.test_client() as client:
            row = client.get('/api/appanalise/hosts/1/stats').get_json()['rows'][0]
            self.assertEqual((row['NU_HOST_FILES'], row['NU_PENDING_FILE_BACKUP_TASKS'],
                row['NU_DONE_FILE_BACKUP_TASKS'], row['VL_PENDING_BACKUP_KB']), (4, 3, 1, 3))
            self.assertIs(type(row['NU_HOST_FILES']), float)
            self.assertIs(type(row['VL_PENDING_BACKUP_KB']), float)
            empty = client.get('/api/appanalise/hosts/2/stats').get_json()['rows'][0]
            self.assertEqual(empty['NU_HOST_FILES'], 0)
            self.assertEqual(client.get('/api/appanalise/hosts/3/stats').get_json()['rows'], [])

    def test_history_transition_keeps_queue_and_history_atomic(self) -> None:
        """Reuse a frozen task and commit its history reset in the same transaction.

        Args:
            None.
        Returns:
            None.
        """
        with psycopg.connect(self.conninfo) as connection:
            connection.execute('''INSERT INTO "BPDATA"."FILE_TASK_HISTORY"
                ("FK_HOST", "NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME", "NU_STATUS_DISCOVERY", "NU_STATUS_BACKUP", "NU_STATUS_PROCESSING")
                VALUES (1, '/input', 'source.bin', 0, 0, -3)''')
            connection.execute('''INSERT INTO "BPDATA"."FILE_TASK"
                ("FK_HOST", "NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME", "NU_TYPE", "NU_STATUS")
                VALUES (1, '/input', 'source.bin', 2, -3)''')
        maintenance = importlib.import_module('modules.tasks.service')
        maintenance_db = importlib.import_module('db.tasks')
        connection = self.db.get_connection_bpdata()
        try:
            original = maintenance_db._prepare_history_file_task

            def fail_after_queue_write(*args, **kwargs):
                """Fail between queue and history writes. Args: forwarded. Returns: None."""
                original(*args, **kwargs)
                raise RuntimeError('History persistence failed')

            with patch.object(maintenance_db, '_prepare_history_file_task', side_effect=fail_after_queue_write):
                with self.assertRaisesRegex(RuntimeError, 'History persistence failed'):
                    maintenance.apply_history_action(connection, [1], target_stage='backup', target_status=1)
            before = connection.execute('SELECT "NU_TYPE", "NU_STATUS" FROM "FILE_TASK"').fetchone()
            self.assertEqual(before, {'NU_TYPE': 2, 'NU_STATUS': -3})
            connection.rollback()
            result = maintenance.apply_history_action(connection, [1], target_stage='backup', target_status=1)
            self.assertEqual(result['updated_count'], 1)
            row = connection.execute('SELECT "ID_FILE_TASK", "NU_TYPE", "NU_STATUS" FROM "FILE_TASK"').fetchone()
            self.assertEqual(row, {'ID_FILE_TASK': 1, 'NU_TYPE': 1, 'NU_STATUS': 1})
            history = connection.execute('SELECT "NU_STATUS_BACKUP", "DT_PROCESSED" FROM "FILE_TASK_HISTORY"').fetchone()
            self.assertEqual(history, {'NU_STATUS_BACKUP': 1, 'DT_PROCESSED': None})
            outbox = connection.execute('SELECT "NA_SCOPE_TYPE", "NA_SCOPE_VALUE" FROM "RFFUSION_SUMMARY"."SUMMARY_OUTBOX"').fetchall()
            self.assertEqual(outbox, [{'NA_SCOPE_TYPE': 'host', 'NA_SCOPE_VALUE': '1'}])
            self.assertEqual(len(maintenance.list_file_history(connection, maintenance.build_history_filters({}))), 1)
        finally:
            connection.close()

    def test_observed_identity_preserves_privileges_and_photo(self) -> None:
        """Keep observed profiles separate from privilege grants and deletions.

        Args:
            None.
        Returns:
            None.
        """
        users = importlib.import_module('db.users')
        users.create_webfusion_user(user_name='Test', user_email='test@example.test',
            job_title=None, department=None, location=None, is_admin=True, is_developer=False)
        values = dict(user_name='Updated', user_email='test@example.test', job_title=None,
                      department=None, location=None)
        users.register_observed_user(**values, profile_image_url='https://example.test/photo.jpg')
        users.register_observed_user(**values)
        self.assertEqual(users.get_access_role('test@example.test'), 'admin')
        self.assertEqual(users.get_profile_image_url('test@example.test'), 'https://example.test/photo.jpg')
        self.assertEqual(users.get_webfusion_user('test@example.test')['NA_USER_NAME'], 'Updated')
        self.assertEqual(len(users.list_webfusion_users()), 1)

    def test_privilege_failure_rolls_back_identity(self) -> None:
        """Roll back the identity and first privilege when the second grant fails.

        Args:
            None.
        Returns:
            None.
        """
        users = importlib.import_module('db.users')
        original = users._set_webfusion_role

        def fail_developer(**kwargs):
            """Fail the second role write. Args: forwarded. Returns: None."""
            if kwargs['role'] == 'developer':
                raise RuntimeError('Privilege persistence failed')
            original(**kwargs)

        with patch.object(users, '_set_webfusion_role', side_effect=fail_developer):
            with self.assertRaisesRegex(RuntimeError, 'Privilege persistence failed'):
                users.create_webfusion_user(user_name='Test', user_email='failed@example.test',
                    job_title=None, department=None, location=None, is_admin=True, is_developer=True)
        self.assertIsNone(users.get_webfusion_user('failed@example.test'))
