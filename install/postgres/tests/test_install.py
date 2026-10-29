"""Service-free tests for the PostgreSQL installation package."""

import ast
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
REPOSITORY = PACKAGE.parents[1]


class PackageTests(unittest.TestCase):
    """Validate immutable artifacts and invoke scripts only with fake tools."""

    def setUp(self) -> None:
        """Create isolated state and a fail-closed fake command search path."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls"
        self.calls.touch()
        for name in ("podman", "psql", "initdb", "pg_ctl", "runuser"):
            command = self.bin / name
            command.write_text(
                "#!/bin/bash\nprintf '%s\\n' \"${0##*/} $*\" >> \"$FAKE_CALLS\"\n"
                "printf 'Unexpected service access in fake: %s\\n' \"${0##*/}\" >&2\nexit 91\n"
            )
            command.chmod(0o755)
        self.env = {
            "PATH": f"{self.bin}:/usr/bin:/bin",
            "HOME": str(self.root),
            "FAKE_CALLS": str(self.calls),
            "RFFUSION_ENV": "development",
            "RFFUSION_COMPONENT": "postgres",
        }

    def run_script(self, name: str, *args: str) -> subprocess.CompletedProcess:
        """Run one package script with fail-closed doubles and a bounded timeout."""
        self.assertTrue((PACKAGE / name).is_file(), name)
        return subprocess.run(
            ["bash", str(PACKAGE / name), *args],
            env=self.env, text=True, capture_output=True, timeout=10,
        )

    def test_identity_accepts_crlf(self) -> None:
        config = self.root / ".env"
        config.mkdir()
        (config / "runtime.env").write_bytes(b"# local\r\nRFFUSION_ENV=development\r\n")
        result = subprocess.run(
            ["bash", "-c", '. "$1"; postgres_load_environment "$2"; postgres_validate_identity',
             "test", str(PACKAGE / "linux/environment.sh"), str(self.root)],
            env=self.env, text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls.read_text(), "")

    def test_identity_requires_configuration_file(self) -> None:
        result = subprocess.run(
            ["bash", "-c", '. "$1"; postgres_load_environment "$2"',
             "test", str(PACKAGE / "linux/environment.sh"), str(self.root)],
            env=self.env, text=True, capture_output=True, timeout=5,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Configure", result.stderr)
        self.assertEqual(self.calls.read_text(), "")

    def test_interactive_prompt_is_idempotent(self) -> None:
        profile = self.root / "profile.sh"
        result = subprocess.run(
            ["bash", "-c", '. "$1"; postgres_prompt_profile',
             "test", str(PACKAGE / "linux/environment.sh")],
            env=self.env, text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        profile.write_text(result.stdout)
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-i", "-c",
             '. "$1"; . "$1"; printf "%s\\n" "$PS1"', "test", str(profile)],
            env=self.env, text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count("[RF.Fusion development postgres]"), 1)
        self.assertEqual(self.calls.read_text(), "")

    def test_password_secret_validation_without_disclosure(self) -> None:
        first = self.root / "rffusion-postgres-appcataloga"
        second = self.root / "rffusion-postgres-webfusion"
        first.write_bytes(b"fixture-only-one\n")
        for value, expected in ((b"fixture-only-two", True), (b"", False),
                                (b"one\ntwo", False), (b"one\n\n", False),
                                (b"one\r", False), (b"one\0two", False),
                                (b"fixture-only-one", False)):
            with self.subTest(expected=expected, size=len(value)):
                second.write_bytes(value)
                result = subprocess.run(
                    ["bash", "-c", 'set -Eeuo pipefail; . "$1"; postgres_validate_password_secrets "$2"',
                     "test", str(PACKAGE / "linux/common.sh"), str(self.root)],
                    env=self.env, text=True, capture_output=True, timeout=5,
                )
                self.assertEqual(result.returncode == 0, expected, result.stderr)
                self.assertNotIn("fixture-only", result.stdout + result.stderr)
        self.assertEqual(self.calls.read_text(), "")

    def test_metadata_rejects_foreign_or_incomplete_target(self) -> None:
        import copy
        entry = {
            "Name": "debian12-postgres", "State": {"Running": True},
            "Config": {"Labels": {"io.rffusion.component": "postgres",
                                  "io.rffusion.environment": "development"},
                       "Env": ["RFFUSION_COMPONENT=postgres", "RFFUSION_ENV=development"]},
            "Mounts": [
                {"Type": "volume", "Name": "rffusion-postgres-development-data", "Destination": "/var/lib/postgresql/data"},
                {"Type": "volume", "Name": "rffusion-postgres-development-state", "Destination": "/srv/app_volume"},
                {"Type": "bind", "Destination": "/RFFusion", "RW": False},
            ],
            "HostConfig": {"PortBindings": {"5432/tcp": [{"HostPort": "9083"}], "2828/tcp": [{"HostPort": "2226"}]}},
        }
        cases = [(entry, True)]
        for field in ("name", "production", "environment", "mount", "repository", "port", "stopped"):
            altered = copy.deepcopy(entry)
            if field == "name":
                altered["Name"] = "another"
            elif field == "production":
                altered["Config"]["Labels"]["io.rffusion.environment"] = "production"
            elif field == "environment":
                altered["Config"]["Env"] = []
            elif field == "mount":
                altered["Mounts"][0]["Name"] = "foreign"
            elif field == "repository":
                altered["Mounts"][2]["RW"] = True
            elif field == "port":
                altered["HostConfig"]["PortBindings"] = {}
            else:
                altered["State"]["Running"] = False
            cases.append((altered, False))
        for value, expected in cases:
            with self.subTest(value=value["Name"], expected=expected):
                result = subprocess.run(
                    [sys.executable, "-B", str(PACKAGE / "linux/inspect.py"),
                     "target", "--name", "debian12-postgres"],
                    input=json.dumps([value]), env=self.env, text=True, capture_output=True, timeout=5,
                )
                self.assertEqual(result.returncode == 0, expected, result.stderr)

    def test_identity_rejects_invalid_or_executable_input(self) -> None:
        config = self.root / ".env"
        config.mkdir()
        for value in ("", "RFFUSION_ENV=bad", "RFFUSION_ENV=production\nRFFUSION_ENV=development",
                      'RFFUSION_ENV=$(touch EXPLOIT)', "OTHER=development"):
            with self.subTest(value=value):
                (config / "runtime.env").write_text(value)
                result = subprocess.run(
                    ["bash", "-c", '. "$1"; postgres_load_environment "$2"',
                     "test", str(PACKAGE / "linux/environment.sh"), str(self.root)],
                    env=self.env, text=True, capture_output=True, timeout=5,
                )
                self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "EXPLOIT").exists())
        self.assertEqual(self.calls.read_text(), "")

    def test_deploy_rejects_arguments_before_podman(self) -> None:
        for args in (("--unknown",), ("--check", "--check"), ("--repo",), ("--repo", "")):
            with self.subTest(args=args):
                result = self.run_script("linux/deploy-debian12-postgres.sh", *args)
                self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls.read_text(), "")

    def test_recreate_stops_at_a1_before_services(self) -> None:
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--recreate")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("A1", result.stderr)
        self.assertEqual(self.calls.read_text(), "")

    def test_integration_requires_explicit_target_and_ack(self) -> None:
        for args in ((), ("--ack-disposable",), ("--container",),
                     ("--container", ""), ("--container", "db"),
                     ("--container", "db", "--ack-disposable", "--ack-disposable"),
                     ("--container", "db", "--container", "db", "--ack-disposable"),
                     ("--container", "db", "--ack-disposable", "--unknown"),
                     ("--container", "*", "--ack-disposable")):
            with self.subTest(args=args):
                result = self.run_script("tests/integration.sh", *args)
                self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls.read_text(), "")

    def test_snapshot_inventory(self) -> None:
        manifest = json.loads((PACKAGE / "sql/object-map.json").read_text())
        tables = manifest["tables"]
        self.assertEqual(len(tables), 40)
        self.assertEqual(sum(len(table["columns"]) for table in tables), 451)
        self.assertEqual(sum(len(table["foreign_keys"]) for table in tables), 22)
        self.assertEqual(sum(column["generated"] for table in tables for column in table["columns"]), 23)
        self.assertEqual(sum(column["on_update"] for table in tables for column in table["columns"]), 5)
        self.assertEqual(sum(len(table["unique"]) for table in tables), 8)
        self.assertEqual(sum(index["kind"] == "btree" for table in tables for index in table["indexes"]), 52)
        self.assertEqual(sum(index["kind"] == "gin" for table in tables for index in table["indexes"]), 4)
        self.assertEqual(sum(index["kind"] == "gist" for table in tables for index in table["indexes"]), 1)
        for table in tables:
            self.assertTrue(table["primary_key"])
            self.assertTrue(all(column["test"] for column in table["columns"]))

    def test_physical_object_names_are_unique_and_bounded(self) -> None:
        manifest = json.loads((PACKAGE / "sql/object-map.json").read_text())
        seen = set()
        for table in manifest["tables"]:
            names = [table["name"], table["name"] + "_pkey"]
            names.extend(index["physical"] for index in table["indexes"])
            names.extend(unique["physical"] for unique in table["unique"] if unique["mechanism"] == "unique")
            names.extend(column["sequence"] for column in table["columns"] if column["generated"])
            for name in names:
                self.assertLessEqual(len(name.encode("utf-8")), 63, name)
                qualified = (table["database"], name)
                self.assertNotIn(qualified, seen)
                seen.add(qualified)

    def test_source_and_copied_csv_hashes(self) -> None:
        manifest = json.loads((PACKAGE / "sql/object-map.json").read_text())
        for source in manifest["sources"]:
            with self.subTest(source=source["file"]):
                data = (REPOSITORY / "src/mariadb/scripts" / source["file"]).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), source["sha256"])
                if source["file"].endswith(".csv"):
                    self.assertEqual(data, (PACKAGE / "data" / source["file"]).read_bytes())

    def test_reference_values_and_count(self) -> None:
        manifest = json.loads((PACKAGE / "sql/object-map.json").read_text())
        self.assertEqual(sum(seed["rows"] for seed in manifest["seeds"]), 5618)
        for seed in manifest["seeds"]:
            with (PACKAGE / "data" / seed["file"]).open(newline="") as stream:
                rows = list(csv.reader(stream))
            self.assertEqual(len(rows) - 1, seed["rows"])
            self.assertEqual(len(rows[0]), len(seed["columns"]))

    def test_no_credentials_or_unconverted_dialect(self) -> None:
        for path in (PACKAGE / "sql").glob("create*.sql"):
            text = path.read_text()
            self.assertNotRegex(text, r"(?i)\b(AUTO_INCREMENT|ENGINE=|LOAD DATA|USING HASH|IFNULL)\b")
            for command in (
                "\\set app_password `cat /run/secrets/rffusion-postgres-appcataloga`",
                "\\set web_password `cat /run/secrets/rffusion-postgres-webfusion`",
            ):
                text = text.replace(command, "")
            self.assertNotIn("`", text)
        text = (PACKAGE / "linux/Containerfile").read_text()
        self.assertIn("postgresql-18", text)
        self.assertNotIn("COPY . ", text)
        self.assertNotRegex(text, r"(?i)password\s*=\s*['\"]")

    def test_bash_syntax(self) -> None:
        for path in list((PACKAGE / "linux").glob("*.sh")) + list((PACKAGE / "tests").glob("*.sh")):
            with self.subTest(path=path.name):
                result = subprocess.run(["bash", "-n", str(path)], env=self.env,
                                        text=True, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_python_and_embedded_concurrency_syntax(self) -> None:
        for path in PACKAGE.rglob("*.py"):
            with self.subTest(path=path.name):
                ast.parse(path.read_text(), filename=str(path))
        script = (PACKAGE / "tests/concurrency.sh").read_text()
        embedded = script.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        ast.parse(embedded, filename="concurrency.sh:python")

    def install_preflight_fakes(self) -> Path:
        """Create a complete local repo fixture and deterministic fake host metadata."""
        repo = self.root / "repository"
        (repo / ".env").mkdir(parents=True)
        (repo / ".env/runtime.env").write_text("RFFUSION_ENV=development\n")
        (repo / "AGENTS.md").write_text("test repository\n")
        sources = repo / "src/mariadb/scripts"
        sources.mkdir(parents=True)
        manifest = json.loads((PACKAGE / "sql/object-map.json").read_text())
        for source in manifest["sources"]:
            shutil.copyfile(REPOSITORY / "src/mariadb/scripts" / source["file"], sources / source["file"])
        podman = self.bin / "podman"
        podman.write_text(f"#!{sys.executable}\n" + r'''
import json, os, sys
args=sys.argv[1:]
assert args.pop(0) == "--remote=false"
with open(os.environ["FAKE_CALLS"],"a") as out: out.write(json.dumps(args)+"\n")
case=os.environ.get("FAKE_CASE","ok")
if args[:2]==["container","exists"]:
    sys.exit(0 if case=="exists" else 1)
if args[0]=="info":
    print(json.dumps({"host":{"cgroupVersion":"v2","cgroupControllers":["memory","pids"] if case=="controllers" else ["cpu","memory","pids"],
                             "security":{"rootless":True}},"store":{"graphRoot":os.environ["HOME"]}}))
elif args[:2]==["network","inspect"]:
    print(json.dumps([{"name":"podman","subnets":[{"subnet":"10.90.0.0/16" if case=="network" else "10.88.0.0/16"}]}]))
elif args[0]=="ps": pass
elif args[:2]==["secret","inspect"]:
    if case=="secret": sys.exit(125)
    print("{}")
elif args[:2]==["volume","exists"]: sys.exit(0 if case=="volume" else 1)
elif args[:2]==["volume","inspect"]:
    print(json.dumps([{"Name":name,"Labels":{"io.rffusion.component":"other" if case=="race" else "postgres",
          "io.rffusion.environment":"development","io.rffusion.pg-major":"18"}} for name in args[2:]]))
elif args[0]=="build": sys.exit(125 if case=="build" else 0)
elif args[:2]==["volume","create"]: print(args[-1])
elif args[0]=="run": print("fake-container-id")
elif args[0]=="inspect": print("running healthy" if "Health.Status" in args[2] else "{}")
elif args[0]=="exec": pass
else: raise SystemExit("Unexpected fake Podman command")
''')
        podman.chmod(0o755)
        for tool, body in {
            "ip": '[[ "${FAKE_CASE:-}" != ip ]] || echo "1: eth0 inet 10.88.0.35/16"; exit 0',
            "ss": '[[ "${FAKE_CASE:-}" != port ]] || echo "LISTEN 0 128 0.0.0.0:9083 0.0.0.0:*"; exit 0',
            "getenforce": 'if [[ "${FAKE_CASE:-}" == selinux ]]; then echo Enforcing; else echo Disabled; fi',
        }.items():
            path = self.bin / tool
            path.write_text("#!/bin/bash\n" + body + "\n")
            path.chmod(0o755)
        return repo

    def test_check_is_non_mutating_with_valid_host(self) -> None:
        repo = self.install_preflight_fakes()
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo), "--check")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertFalse(any(call[0] in ("build", "run", "rm", "stop") or call[:2] == ["volume","create"] for call in calls))

    def test_preflight_failures_never_mutate(self) -> None:
        repo = self.install_preflight_fakes()
        for case in ("exists", "network", "secret", "volume", "port", "ip", "selinux"):
            with self.subTest(case=case):
                self.calls.write_text("")
                self.env["FAKE_CASE"] = case
                result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo), "--check")
                self.assertNotEqual(result.returncode, 0)
                calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
                self.assertFalse(any(call[0] in ("build", "run", "rm", "stop") for call in calls))

    def test_invalid_config_and_remote_context_precede_podman(self) -> None:
        repo = self.install_preflight_fakes()
        self.env["CONTAINER_HOST"] = "unix:///unavailable-test-context.sock"
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo), "--check")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls.read_text(), "")
        del self.env["CONTAINER_HOST"]
        (repo / ".env/runtime.env").write_text("RFFUSION_ENV=invalid\n")
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo), "--check")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls.read_text(), "")

    def test_failed_build_does_not_create_or_remove_resources(self) -> None:
        repo = self.install_preflight_fakes()
        self.env["FAKE_CASE"] = "build"
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo))
        self.assertNotEqual(result.returncode, 0)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(any(call[0] == "build" for call in calls))
        self.assertFalse(any(call[0] in ("run", "rm", "stop") or call[:2] == ["volume","create"] for call in calls))

    def test_successful_fresh_run_uses_named_mounts_and_no_password_argv(self) -> None:
        repo = self.install_preflight_fakes()
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo))
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        run = next(call for call in calls if call[0] == "run")
        self.assertIn("rffusion-postgres-development-data:/var/lib/postgresql/data", run)
        self.assertIn("rffusion-postgres-development-state:/srv/app_volume", run)
        self.assertIn(str(repo) + ":/RFFusion:ro", run)
        self.assertIn("9083:5432", run)
        self.assertIn("2226:2828", run)
        self.assertIn("10.88.0.35", run)
        self.assertFalse(any(":Z" in arg or arg.startswith("PGPASSWORD=") for arg in run))
        self.assertFalse(any(call[0] in ("rm", "stop") for call in calls))

    def test_foreign_volume_after_creation_never_reaches_run(self) -> None:
        repo = self.install_preflight_fakes()
        self.env["FAKE_CASE"] = "race"
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo))
        self.assertNotEqual(result.returncode, 0)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(any(call[:2] == ["volume","inspect"] for call in calls))
        self.assertFalse(any(call[0] in ("run", "rm", "stop") for call in calls))

    def test_missing_controller_is_reported_as_omitted(self) -> None:
        repo = self.install_preflight_fakes()
        self.env["FAKE_CASE"] = "controllers"
        result = self.run_script("linux/deploy-debian12-postgres.sh", "--repo", str(repo), "--check")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CPU limit omitted", result.stderr)
        self.assertNotIn("--cpus=1", result.stdout)


if __name__ == "__main__":
    unittest.main()
