# Harness Engineering

## Purpose

This document defines the local validation harness used before merge and deployment.

## Mandatory checks

Run from repo root:

```bash
uv run pytest -q
```

Optional quality gate:

```bash
PYTHONPATH="${OPENCODE_HOME:-$HOME/.config/opencode}" \
  python3 -m scripts.lint_by_filetype --json
```

## Required evidence before merge

- Fresh local run with all tests passing.
- Any touched tests updated/added with names matching behavior change.
- No uncommitted generated artifacts (e.g., venv, `__pycache__`, `.pytest_cache`).

## Failure policy

- Test failures: fix implementation and rerun full suite.
- Parser/runtime failures: add a reproducer test first (TDD flow), then patch.
- Regressions in resume semantics: add/extend `tests/test_state.py` and `tests/test_runner.py`.

## Extensibility

If additional integrations are added, extend this file with new harness stages
(e.g., smoke run against a staging endpoint).
