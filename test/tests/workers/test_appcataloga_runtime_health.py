"""Tests for the read-only appCataloga runtime health script."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(
    "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shell/"
    "appCataloga_runtime_health.sh"
)


class TestAppCatalogaRuntimeHealth(unittest.TestCase):
    """Keep worker details in the health payload useful for operations."""

    def test_backup_worker_count_and_script_are_reported(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            pgrep_path = Path(temporary_dir) / "pgrep"
            pgrep_path.write_text(
                "#!/usr/bin/env bash\n"
                "case \"${!#}\" in\n"
                "  appCataloga_file_bkp.py) printf '41\\n42\\n43\\n' ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            pgrep_path.chmod(0o755)
            environment = os.environ | {"PATH": f"{temporary_dir}:{os.environ['PATH']}"}

            result = subprocess.run(
                ["bash", str(SCRIPT_PATH)],
                capture_output=True,
                check=True,
                env=environment,
                text=True,
            )

        payload = json.loads(result.stdout)
        checks_by_script = {check["script"]: check for check in payload["checks"]}
        backup_check = checks_by_script["appCataloga_file_bkp.py"]

        self.assertEqual(backup_check["worker_count"], 3)
        self.assertEqual(backup_check["status"], "healthy")
        self.assertEqual(backup_check["detail"], "3 workers em execução")
