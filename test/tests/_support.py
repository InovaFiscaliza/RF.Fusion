"""
Shared test helpers for the appCataloga validation suite.

Usage:
    These helpers are intentionally small and explicit. They exist to make the
    tests independent from IDE state and to load the production modules exactly
    from the source tree under `/RFFusion/src/...`.
"""

from __future__ import annotations

import importlib.util
import importlib
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from unittest.mock import patch


APP_ROOT = Path("/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga")
CONFIG_ROOT = Path("/RFFusion/src/appCataloga/server_volume/etc/appCataloga")
SHARED_ROOT = APP_ROOT / "shared"
DB_ROOT = APP_ROOT / "db"
APPANALISE_ROOT = APP_ROOT / "appAnalise"
HOST_HANDLER_ROOT = APP_ROOT / "host_handler"
SERVER_HANDLER_ROOT = APP_ROOT / "server_handler"
STATIONS_LEGACY_ROOT = APP_ROOT / "stations_legacy"


def ensure_app_paths() -> None:
    """Expose runtime paths and keep the test db package from shadowing runtime data.

    Args:
        None.
    Returns:
        None. The production db package is loaded without handlers or connections.
    """
    for path in (str(CONFIG_ROOT), str(APP_ROOT)):
        if path not in sys.path:
            sys.path.insert(0, path)
    package = sys.modules.get("db")
    if package is None or getattr(package, "__file__", None) != str(DB_ROOT / "__init__.py"):
        load_package_from_dir("db", DB_ROOT)


def load_package_from_dir(package_name: str, package_dir: Path):
    """Load a package from a directory under a custom import alias."""
    init_path = package_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        package_name,
        init_path,
        submodule_search_locations=[str(package_dir)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load package {package_name} from {package_dir}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[package_name] = module
    spec.loader.exec_module(module)
    return module


def import_package_module(
    package_name: str,
    package_dir: Path,
    module_name: str,
):
    """Import a module from a package that was loaded under a custom alias."""
    if package_name not in sys.modules:
        load_package_from_dir(package_name, package_dir)

    return importlib.import_module(f"{package_name}.{module_name}")


def load_module_from_path(module_name: str, path: str):
    """Load a module directly from a file path without importing its package."""
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_test_config() -> ModuleType:
    """Load central constants without reading the runtime credential file.

    Args:
        None.

    Returns:
        ModuleType: Actual appCataloga configuration with synthetic database
        credentials. The module is not registered globally as config.
    """
    with patch(
        "runpy.run_path",
        return_value={"DB_USER_NAME": "unit-test", "DB_PASSWORD": "unit-test"},
    ):
        return load_module_from_path(
            "appcataloga_test_config", str(CONFIG_ROOT / "config.py")
        )


@contextmanager
def bind_test_config() -> Generator[ModuleType, None, None]:
    """Bind synthetic configuration without unloading imported native dependencies.

    Snapshotting all of sys.modules can remove newly loaded extension modules
    that cannot be initialized twice. Restore only the configuration binding;
    never read the runtime credential file to import a database handler in tests.

    Args:
        None.

    Returns:
        Generator[ModuleType, None, None]: Context yielding application constants
        with synthetic credentials, then restoring the prior config binding.
    """
    previous = sys.modules.get("config")
    config = load_test_config()
    sys.modules["config"] = config
    try:
        yield config
    finally:
        if previous is None:
            sys.modules.pop("config", None)
        else:
            sys.modules["config"] = previous


@contextmanager
def bind_real_package(package_name: str, package_dir: Path):
    """
    Temporarily expose a real production package during module loading.

    Why this exists:
        The test tree reuses names like `shared`, `db` and `stations`. Test
        discovery can therefore shadow the real production packages. This
        context manager binds the real package only for the import window
        required by the module under test.
    """
    previous_package = sys.modules.get(package_name)

    real_package = load_package_from_dir(package_name, package_dir)
    try:
        yield real_package
    finally:
        if previous_package is not None:
            sys.modules[package_name] = previous_package
        else:
            sys.modules.pop(package_name, None)


@contextmanager
def bind_real_shared_package():
    """Temporarily expose appCataloga's production `shared` package."""
    with bind_real_package("shared", SHARED_ROOT) as package:
        yield package
