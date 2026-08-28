from __future__ import annotations

import json
import pathlib
from typing import Any


def update_ledger(path: pathlib.Path, section: str, value: Any) -> None:
    payload: dict[str, Any] = {}
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError(f"run ledger must contain an object: {path}")
        payload = loaded
    payload[section] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
