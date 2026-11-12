"""Read explicit PostgreSQL connection settings without opening a session."""

import os
from pathlib import Path
import runpy

_SECRET = runpy.run_path(str(Path(__file__).with_name(".secret")))
PG_CONNINFO = os.environ.get("RFFUSION_PG_CONNINFO") or _SECRET.get("PG_CONNINFO", "")
PG_APPLICATION_NAME = "webfusion"
