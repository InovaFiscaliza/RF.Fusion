"""Protect the conversion rules of the MariaDB dump migration without databases."""

from __future__ import annotations

import gzip
import importlib.util
from pathlib import Path
import struct
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "mariadb_dump_to_postgres", ROOT / "src/postgres/migration/mariadb_dump_to_postgres.py",
)
migration = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migration
SPEC.loader.exec_module(migration)


def mysql_point(x: float, y: float, srid: int = 0) -> bytes:
    """Build a MariaDB internal point: SRID followed by little-endian WKB.

    Args:
        x (float): Longitude.
        y (float): Latitude.
        srid (int): Spatial reference identifier.

    Returns:
        bytes: Internal geometry value.
    """
    return srid.to_bytes(4, "little") + b"\x01" + (1).to_bytes(4, "little") + struct.pack("<dd", x, y)


class TestMariaDbDumpMigration(unittest.TestCase):
    """Check parsing, value conversion and inspection of a small gzip dump."""

    def write_dump(self, text: str) -> Path:
        """Write a temporary gzip dump.

        Args:
            text (str): Dump content.

        Returns:
            Path: Compressed dump path, removed after the test.
        """
        folder = tempfile.TemporaryDirectory(prefix="rffusion-dump-test-")
        self.addCleanup(folder.cleanup)
        path = Path(folder.name) / "dump.sql.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            stream.write(text)
        return path

    def test_values_are_decoded_and_escaped_for_copy(self) -> None:
        table = migration.DumpTable("BPDATA", "T", ["ID", "NA", "DT", "JS"],
                                    ["number", "text", "temporal", "json"], None)
        statement = migration.InsertStatement(
            "BPDATA", "T", None,
            "(1,'a\\'b\\\\c\\nd\te','2026-01-02 03:04:05',NULL),\n(-2,'',NULL,'{\\\"k\\\": 1}');\n", 1,
        )
        converter = migration.RowConverter(table, validate_json=True, unique_key=None, keep_rows=False)

        lines = [converter.convert(tokens, 1) for tokens in migration.iter_rows(statement)]

        self.assertEqual(lines, [
            "1\ta'b\\\\c\\nd\\te\t2026-01-02 03:04:05\t\\N\n",
            '-2\t\t\\N\t{"k": 1}\n',
        ])
        self.assertEqual(converter.stats.rows, 2)
        self.assertEqual(converter.stats.first_column_sum, -1)

    def test_point_keeps_coordinates_order_and_known_srid(self) -> None:
        plain, srid, x, y = migration.mysql_geometry_to_ewkb(mysql_point(-48.8, -26.3))
        self.assertEqual((srid, x, y), (0, -48.8, -26.3))
        self.assertEqual(plain, mysql_point(-48.8, -26.3)[4:].hex().upper())

        ewkb, srid, _, _ = migration.mysql_geometry_to_ewkb(mysql_point(-48.8, -26.3, 4326))
        raw = bytes.fromhex(ewkb)
        self.assertEqual(srid, 4326)
        self.assertEqual(int.from_bytes(raw[1:5], "little"), 1 | 0x20000000)
        self.assertEqual(int.from_bytes(raw[5:9], "little"), 4326)
        self.assertEqual(struct.unpack("<dd", raw[9:]), (-48.8, -26.3))

        not_a_point = b"\x00" * 4 + b"\x01" + (2).to_bytes(4, "little") + b"\x00" * 16
        with self.assertRaises(migration.MigrationError):
            migration.mysql_geometry_to_ewkb(not_a_point)

    def test_malformed_statement_fails_instead_of_dropping_rows(self) -> None:
        statement = migration.InsertStatement("RFDATA", "T", None, "(1,'a') garbage (2,'b');\n", 7)
        with self.assertRaisesRegex(migration.MigrationError, "line 7"):
            list(migration.iter_rows(statement))

    def test_inspect_reports_postgres_only_duplicates_and_bad_values(self) -> None:
        site = mysql_point(-60.0, -3.1).hex().upper()
        path = self.write_dump(
            "USE `RFDATA`;\n"
            "CREATE TABLE `DIM_SPECTRUM_EQUIPMENT` (\n"
            "  `ID_EQUIPMENT` int(11) NOT NULL AUTO_INCREMENT,\n"
            "  `NA_EQUIPMENT` varchar(100) DEFAULT NULL,\n"
            "  PRIMARY KEY (`ID_EQUIPMENT`)\n"
            ") ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4;\n"
            "INSERT INTO `DIM_SPECTRUM_EQUIPMENT` VALUES (1,'RFEye'),(2,'rfeye'),(3,'RFEye');\n"
            "CREATE TABLE `DIM_SPECTRUM_SITE` (\n"
            "  `ID_SITE` int(11) NOT NULL,\n"
            "  `GEO_POINT` point NOT NULL,\n"
            "  `JS_METADATA` longtext DEFAULT NULL CHECK (json_valid(`JS_METADATA`)),\n"
            "  `NA_SITE` varchar(100) DEFAULT NULL\n"
            ") ENGINE=InnoDB;\n"
            "INSERT INTO `DIM_SPECTRUM_SITE` VALUES\n"
            f"(10,0x{site},'{{}}','a\\0b'),\n"
            f"(11,0x{site},'not json',NULL);\n"
            "USE `RFFUSION_SUMMARY`;\n"
            "CREATE TABLE `HOST_METRIC` (\n  `FK_HOST` int(11) NOT NULL\n) ENGINE=InnoDB;\n"
            "INSERT INTO `HOST_METRIC` VALUES (1);\n"
        )

        report = migration.inspect_dump(path)

        equipment = report["tables"]["RFDATA.DIM_SPECTRUM_EQUIPMENT"]
        self.assertEqual(equipment["rows"], 3)
        self.assertEqual(equipment["postgres_unique_duplicates"], 1)
        self.assertEqual(equipment["duplicate_samples"], [{"id": "3", "first_id": "1", "value": ["RFEye"]}])
        site_report = report["tables"]["RFDATA.DIM_SPECTRUM_SITE"]
        self.assertEqual(site_report["issues"], {"NUL character": 1, "invalid JSON": 1})
        self.assertEqual(report["skipped"]["RFFUSION_SUMMARY.HOST_METRIC"]["reason"],
                         "Summary table rebuilt by full reconcile")
        self.assertTrue(report["blocking"])


if __name__ == "__main__":
    unittest.main()
