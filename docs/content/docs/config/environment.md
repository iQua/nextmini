---
title: "Config Environment Variables"
description: ""
---

| Variable | Description |
|----------|-------------|
| `CONTROLLER_RESET_DB` | Controller DB bootstrap behavior. If unset, reset is enabled (`true`). If set, only exact values `1`, `true`, or `yes` enable reset; any other value disables reset and applies schema without dropping existing data. |
| `RUST_LOG` | Logging level (`info`, `debug`, `trace`). |
| `PYO3_PYTHON` | Path to Python 3.13 interpreter for `nextmini_py` Rust tests. |
| `DATABASE_URL` | PostgreSQL connection string used by helper scripts such as `utils/start-database.sh` (controller runtime uses `[db]` config fields). |
