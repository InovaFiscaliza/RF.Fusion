"""PostgreSQL persistence and its shared JSON constants."""

import json
from pathlib import Path


constants = json.loads(
    Path(__file__).with_name("constants.json").read_text(encoding="utf-8")
)
