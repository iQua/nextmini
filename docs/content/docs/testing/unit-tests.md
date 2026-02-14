---
title: "Running Unit Tests"
description: "Run Nextmini unit and session tests with the required Python 3.13 environment setup."
---

Before running tests, make sure `PYO3_PYTHON` represents `/path/to/python3.13`, such as the following on macOS:

```
export PYO3_PYTHON=/opt/homebrew/opt/python@3.13/bin/python3.13
```

Controller integration tests that require Postgres also need the database started first (for example, by running `bash ./utils/start-database.sh`).

Then run all tests from the repository root (`nextmini/`):

```bash
cargo nextest run --no-default-features --features python-extension --features dev-tests
```
