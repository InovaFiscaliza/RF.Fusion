"""Exercise PostgreSQL replacement guards without containers or shared databases."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "install/postgres/linux/deploy-postgres.sh"

FAKE_PODMAN = r"""#!/opt/conda/envs/appdata/bin/python
import json, os, pathlib, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ["RFF_TEST_ROOT"])
with (root / "calls.jsonl").open("a") as stream:
    stream.write(json.dumps(args) + "\n")
state = root / "existing"
if args[:2] in (["pod", "exists"], ["container", "exists"]):
    if args[:2] == ["container", "exists"] and os.environ.get("RFF_TEST_INFRA_ONLY") == "1":
        sys.exit(1)
    sys.exit(0 if state.exists() else 1)
if args[:2] == ["pod", "rm"]:
    state.unlink(missing_ok=True)
    sys.exit(0)
if args[:2] == ["pod", "create"]:
    pass
elif args[0] == "info":
    print("false v2 cpu memory pids")
elif args[0] == "inspect":
    print(os.environ.get("RFF_TEST_RUNNING", "true"))
elif args[0] == "pull":
    sys.exit(9 if os.environ.get("RFF_TEST_PULL_FAIL") == "1" else 0)
elif args[0] in ("build", "network", "volume"):
    pass
elif args[0] == "run":
    if "--rm" in args:
        # Helper that seeds the data directory from the former named volume.
        print(os.environ.get("RFF_TEST_SEED", "copied"))
    else:
        state.touch()
elif args[0] == "exec":
    if "pg_dump" in args:
        if os.environ.get("RFF_TEST_BACKUP_FAIL") == "1":
            sys.exit(7)
        print("-- fixture backup" if "--data-only" in args else "fixture archive")
    elif "pg_restore" in args:
        sys.stdin.read()
        print("fixture archive contents")
    elif "pg_isready" in args:
        pass
    elif "psql" in args:
        if args[-2:] == ["-f", "-"]:
            (root / "reset.sql").write_text(sys.stdin.read())
            sys.exit(8 if os.environ.get("RFF_TEST_RESET_FAIL") == "1" else 0)
        query = args[args.index("-c") + 1] if "-c" in args else ""
        if "pg_available_extensions" in query:
            print(os.environ.get("RFF_TEST_POSTGIS", "1"))
        elif "information_schema.tables" in query:
            print(os.environ.get("RFF_TEST_USAGE_TABLES", "2"))
        elif "pg_namespace" in query:
            print("4" if "'BPDATA'" in query else "0")
    else:
        sys.exit(99)
else:
    sys.exit(99)
"""


class PostgresDeployTests(unittest.TestCase):
    """Protect backups, explicit reset intent and transactional usage preservation.

    Attributes:
        root (Path): Isolated repository and fake Podman state.
        environment (dict[str, str]): Child environment with only a fake container CLI.
    """

    def setUp(self) -> None:
        """Prepare a temporary repository and a recording Podman double.

        Args:
            None.
        Returns:
            None.
        """
        folder = tempfile.TemporaryDirectory(prefix="rffusion-postgres-deploy-")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        (self.root / ".env").mkdir()
        (self.root / ".env/runtime.env").write_text("RFFUSION_ENV=development\n")
        (self.root / "existing").touch()
        binaries = self.root / "bin"
        binaries.mkdir()
        cli = binaries / "podman"
        cli.write_text(FAKE_PODMAN)
        cli.chmod(0o755)
        self.environment = {
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "RFFUSION_REPO_ROOT": str(self.root),
            "RFF_TEST_ROOT": str(self.root),
        }

    def _run(self, *options: str, **settings: str) -> subprocess.CompletedProcess[str]:
        """Run only the installer control flow against a fake Podman.

        Args:
            *options (str): Explicit deploy CLI options.
            **settings (str): Fixture failure switches and returned counts.
        Returns:
            subprocess.CompletedProcess[str]: Exit status and captured diagnostics.
        """
        return subprocess.run(
            ["bash", str(DEPLOY), *options],
            env={**self.environment, **settings},
            capture_output=True,
            text=True,
            timeout=20,
        )

    def _calls(self) -> list[list[str]]:
        """Read the fixture's ordered container operations.

        Args:
            None.
        Returns:
            list[list[str]]: CLI argument lists, without environment or credentials.
        """
        return [
            json.loads(line)
            for line in (self.root / "calls.jsonl").read_text().splitlines()
        ]

    def test_existing_container_requires_explicit_replacement(self) -> None:
        """Default execution must preserve the existing installation.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run()
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((self.root / "existing").exists())
        self.assertFalse(
            any(call[0] in ("build", "run", "rm") for call in self._calls())
        )

    def test_backup_failure_prevents_replacement(self) -> None:
        """A failed archive cannot reach build or removal operations.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", RFF_TEST_BACKUP_FAIL="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(
            any(
                call[0] in ("build", "run", "rm") or call[:2] == ["pod", "rm"]
                for call in self._calls()
            )
        )

    def test_recreation_preserves_schemas_and_named_volumes(self) -> None:
        """Image replacement alone cannot erase schema data or named volumes.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self._calls()
        backup = next(i for i, call in enumerate(calls) if "pg_dump" in call)
        build = next(i for i, call in enumerate(calls) if call[0] == "build")
        removal = next(i for i, call in enumerate(calls) if call[:2] == ["pod", "rm"])
        self.assertLess(backup, build)
        self.assertLess(build, removal)
        self.assertFalse((self.root / "reset.sql").exists())
        self.assertFalse(any(call[:2] == ["volume", "rm"] for call in calls))

    def test_data_moves_to_repository_directory_before_the_new_server(self) -> None:
        """Seed the bind-mounted cluster only after the old server is gone.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self._calls()
        removal = next(i for i, call in enumerate(calls) if call[:2] == ["pod", "rm"])
        seed = next(i for i, call in enumerate(calls) if call[0] == "run" and "--rm" in call)
        server = next(i for i, call in enumerate(calls) if call[0] == "run" and "rffusion-postgres" in call)
        self.assertLess(removal, seed)
        self.assertLess(seed, server)
        self.assertIn("rffusion-postgres-data:/source:ro", calls[seed])
        self.assertIn(f"{self.root}/.env/postgres-data:/var/lib/postgresql/data:z", calls[server])
        self.assertTrue((self.root / ".env/postgres-data").is_dir())

        failed = self._run("--recreate", RFF_TEST_SEED="")
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("volume rffusion-postgres-data is unchanged", failed.stderr)
        later = self._calls()[len(calls):]
        self.assertFalse(any(call[:2] == ["pod", "create"] for call in later))

    def test_reset_preserves_current_usage_inside_the_same_transaction(self) -> None:
        """Capture usage under lock, rebuild all schemas and restore it atomically.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", "--reset-schemas")
        self.assertEqual(result.returncode, 0, result.stderr)
        script = (self.root / "reset.sql").read_text()
        self.assertLess(script.index("LOCK TABLE"), script.index("DROP SCHEMA"))
        self.assertLess(script.index("CREATE TEMP TABLE"), script.index("DROP SCHEMA"))
        self.assertLess(
            script.index("createWebFusionDB.sql"),
            script.index('INSERT INTO "RFFUSION_SUMMARY"'),
        )
        self.assertEqual(script.count("DROP SCHEMA IF EXISTS"), 4)
        self.assertEqual(script.count('INSERT INTO "RFFUSION_SUMMARY"'), 2)
        call = next(call for call in self._calls() if call[-2:] == ["-f", "-"])
        self.assertIn("--single-transaction", call)
        self.assertIn("ON_ERROR_STOP=1", call)
        backups = list(
            (self.root / ".env/postgres-backups").glob("*/postgres-before.dump")
        )
        self.assertEqual(len(backups), 2)
        self.assertTrue(all(path.stat().st_mode & 0o077 == 0 for path in backups))

    def test_missing_postgis_prevents_schema_reset(self) -> None:
        """Do not send destructive SQL when the image lacks its spatial dependency.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", "--reset-schemas", RFF_TEST_POSTGIS="0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PostGIS is missing", result.stderr)
        self.assertFalse((self.root / "reset.sql").exists())

    def test_incomplete_usage_pair_prevents_replacement(self) -> None:
        """A partial usage schema requires inspection before the old pod is removed.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", "--reset-schemas", RFF_TEST_USAGE_TABLES="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call[:2] == ["pod", "rm"] for call in self._calls()))

    def test_reset_failure_preserves_archives_and_reports_rollback(self) -> None:
        """Keep the recovery archive and stop deployment after transactional failure.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", "--reset-schemas", RFF_TEST_RESET_FAIL="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("rolled back", result.stderr)
        self.assertTrue(
            list((self.root / ".env/postgres-backups").glob("*/postgres-before.dump"))
        )
        runs = [call for call in self._calls() if call[0] == "run" and "--rm" not in call]
        self.assertEqual(len(runs), 1)

    def test_infra_has_readable_identity_image_and_command(self) -> None:
        """Keep the pod infrastructure identifiable alongside its services.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self._calls()
        creation = next(call for call in calls if call[:2] == ["pod", "create"])
        self.assertEqual(
            creation[creation.index("--infra-name") + 1], "rffusion-postgres-infra"
        )
        self.assertEqual(
            creation[creation.index("--infra-image") + 1], "registry.k8s.io/pause:3.10"
        )
        self.assertEqual(creation[creation.index("--infra-command") + 1], "/pause")
        pull = next(i for i, call in enumerate(calls) if call[0] == "pull")
        removal = next(i for i, call in enumerate(calls) if call[:2] == ["pod", "rm"])
        self.assertLess(pull, removal)

    def test_infra_pull_failure_keeps_existing_pod(self) -> None:
        """A registry failure must not leave the previous installation removed.

        Args:
            None.
        Returns:
            None.
        """
        result = self._run("--recreate", RFF_TEST_PULL_FAIL="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call[:2] == ["pod", "rm"] for call in self._calls()))

    def test_partial_or_stopped_pod_recovers_before_backing_up(self) -> None:
        """Recover services on retained volumes and back up before any schema reset.

        Args:
            None.
        Returns:
            None.
        """
        for settings in ({"RFF_TEST_INFRA_ONLY": "1"}, {"RFF_TEST_RUNNING": "false"}):
            with self.subTest(settings=settings):
                (self.root / "calls.jsonl").unlink(missing_ok=True)
                result = self._run("--recreate", **settings)
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = self._calls()
                startup = next(
                    i
                    for i, call in enumerate(calls)
                    if call[0] == "run" and "rffusion-postgres" in call
                )
                backup = next(i for i, call in enumerate(calls) if "pg_dump" in call)
                self.assertLess(startup, backup)
                self.assertFalse(any(call[:2] == ["volume", "rm"] for call in calls))
                self.assertFalse((self.root / "reset.sql").exists())
