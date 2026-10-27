"""Validate container identity using temporary files and a fake Podman."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
LOADER = ROOT / "install/common/environment.sh"
IDENTITY = ROOT / "install/common/container-identity.sh"
DEPLOYS = (
    "install/appCataloga/linux/deploy-debian12-python.sh",
    "install/mariaDB/linux/deploy-debian12-mariadb.sh",
    "install/webserver/deploy-rffusionweb.sh",
)


class TestEnvironment(unittest.TestCase):
    """Protect deployment validation and shell identity without real services.

    Attributes:
        root: Temporary filesystem for configuration and generated files. Path.
    """

    def setUp(self) -> None:
        """Create an isolated filesystem.

        Returns:
            None.
        """
        folder = tempfile.TemporaryDirectory(prefix="rffusion-environment-test-")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        (self.root / ".env").mkdir()

    def _load(self, component: str = "appcataloga") -> subprocess.CompletedProcess[str]:
        """Load local configuration without running a deployment.

        Args:
            component: Component whose Podman identity will be built. str.

        Returns:
            Captured Bash result with generated arguments or validation error.
            subprocess.CompletedProcess[str].
        """
        return subprocess.run(
            ["bash", "-c", 'set -Eeuo pipefail; source "$1"; '
             'rffusion_load_environment "$2" "$3"; '
             'printf "%s\\n" "${rffusion_identity_args[@]}"',
             "bash", str(LOADER), str(self.root), component],
            capture_output=True, text=True,
            env={**os.environ, "RFFUSION_ENV": "production"},
        )

    def test_both_environments_identify_all_three_components(self) -> None:
        """Generate consistent hostnames, labels and variables.

        Returns:
            None.
        """
        for environment, suffix in (("production", "prod"), ("development", "dev")):
            for component in ("appcataloga", "webfusion", "mariadb"):
                with self.subTest(environment=environment, component=component):
                    (self.root / ".env/runtime.env").write_text(
                        f"# Local identity\r\n\r\n RFFUSION_ENV = {environment} \r\n"
                    )
                    result = self._load(component)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(f"{component}-{suffix}\n", result.stdout)
                    self.assertIn(f"io.rffusion.environment={environment}\n", result.stdout)
                    self.assertIn(f"io.rffusion.component={component}\n", result.stdout)
                    self.assertIn(f"RFFUSION_ENV={environment}\n", result.stdout)
                    self.assertIn(f"RFFUSION_COMPONENT={component}\n", result.stdout)

    def test_invalid_configuration_never_executes_shell_or_uses_inherited_default(self) -> None:
        """Reject invalid, duplicate, missing and executable configuration.

        Returns:
            None.
        """
        self.assertNotEqual(self._load().returncode, 0)
        marker = self.root / "executed"
        for content in (
            "", "# No environment\n", "RFFUSION_ENV=CHANGE_ME\n",
            "RFFUSION_ENV=production\nRFFUSION_ENV=development\n",
            "RFFUSION_ENV=production\nOTHER_KEY=value\n",
            'RFFUSION_ENV="production"\n',
            f"RFFUSION_ENV=$(touch {marker})\n",
        ):
            with self.subTest(content=content):
                (self.root / ".env/runtime.env").write_text(content)
                self.assertNotEqual(self._load().returncode, 0)
                self.assertFalse(marker.exists())

    def test_all_deploys_reject_bad_configuration_before_podman(self) -> None:
        """Ensure invalid identity cannot reach build or container replacement.

        Returns:
            None.
        """
        common = self.root / "install/common"
        common.mkdir(parents=True)
        shutil.copyfile(LOADER, common / LOADER.name)
        binaries = self.root / "bin"
        binaries.mkdir()
        marker = self.root / "podman-called"
        podman = binaries / "podman"
        podman.write_text('#!/bin/sh\nprintf called > "$RFF_TEST_PODMAN_CALLS"\nexit 99\n')
        podman.chmod(0o755)
        for relative in DEPLOYS:
            script = self.root / relative
            script.parent.mkdir(parents=True, exist_ok=True)
            script.write_text((ROOT / relative).read_text().replace(
                'repoRoot="/RFFusion-dev/RF.Fusion"', f'repoRoot="{self.root}"'
            ))
            for content in (None, "RFFUSION_ENV=invalid\n"):
                with self.subTest(script=relative, content=content):
                    config = self.root / ".env/runtime.env"
                    if content is None:
                        config.unlink(missing_ok=True)
                    else:
                        config.write_text(content)
                    result = subprocess.run(
                        ["bash", str(script)], capture_output=True, text=True,
                        env={**os.environ, "PATH": f"{binaries}:{os.environ['PATH']}",
                             "RFF_TEST_PODMAN_CALLS": str(marker)},
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("ERROR:", result.stderr)
                    self.assertFalse(marker.exists(), relative)

    def test_runtime_identity_is_idempotent_and_reaches_user_shells(self) -> None:
        """Install shell hooks once and preserve existing bashrc content.

        Returns:
            None.
        """
        user_bashrc = self.root / "home/operator/.bashrc"
        user_bashrc.parent.mkdir(parents=True)
        user_bashrc.write_text("# Existing user configuration\n")
        env = {**os.environ, "RFFUSION_ENV": "production", "RFFUSION_COMPONENT": "mariadb"}
        for _ in range(2):
            result = subprocess.run(
                ["bash", str(IDENTITY), str(self.root)], env=env,
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        for relative in ("etc/bash.bashrc", "etc/skel/.bashrc", "root/.bashrc", "home/operator/.bashrc"):
            self.assertEqual((self.root / relative).read_text().count("|| . /etc/profile.d/"), 1)
        self.assertTrue(user_bashrc.read_text().startswith("# Existing user configuration\n"))
        self.assertEqual((self.root / "etc/rffusion-environment").read_text(),
                         "RFFUSION_ENV=production\nRFFUSION_COMPONENT=mariadb\n")

    def test_interactive_prompt_displays_environment_with_optional_color(self) -> None:
        """Show the correct environment without contaminating noninteractive output.

        Returns:
            None.
        """
        for environment, label, color in (("production", "PRODUÇÃO", "31"), ("development", "DEV", "32")):
            env = {**os.environ, "RFFUSION_ENV": environment, "RFFUSION_COMPONENT": "webfusion"}
            subprocess.run(["bash", str(IDENTITY), str(self.root)], env=env,
                           capture_output=True, text=True, check=True)
            profile = str(self.root / "etc/profile.d/rffusion-environment.sh")
            for term in ("xterm", "dumb"):
                with self.subTest(environment=environment, term=term):
                    result = subprocess.run(
                        ["bash", "--noprofile", "--norc", "-ic", '. "$1"; printf "%s" "$PS1"', "bash", profile],
                        env={**env, "TERM": term}, capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(f"[{label}]", result.stdout)
                    self.assertIn(r"\u@\h:\w\$", result.stdout)
                    self.assertEqual(f"1;{color}m" in result.stdout, term == "xterm")
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-c", '. "$1"; printf "%s" "$RFFUSION_ENV"', "bash", profile],
                capture_output=True, text=True,
            )
            self.assertEqual(result.stdout, environment)

    def test_invalid_runtime_identity_does_not_write_files(self) -> None:
        """Fail before installing profiles when runtime variables are invalid.

        Returns:
            None.
        """
        for environment, component in (("", "webfusion"), ("invalid", "mariadb"), ("production", "unknown")):
            with self.subTest(environment=environment, component=component):
                result = subprocess.run(
                    ["bash", str(IDENTITY), str(self.root)], capture_output=True, text=True,
                    env={**os.environ, "RFFUSION_ENV": environment, "RFFUSION_COMPONENT": component},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / "etc").exists())


if __name__ == "__main__":
    unittest.main()
