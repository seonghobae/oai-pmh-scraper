# Deploy Runbook

## Pre-deploy checks

- Confirm `uv run pytest -q` passes on clean working tree.
- Confirm required environment variables for target mode:
  - required OAI variables and optional Snowflake credentials.
- Verify state file path is writable for the deploy user.

## Deployment steps

1. Install/update dependencies (`uv sync` or package manager equivalent).
1. Ensure package entrypoint `oai-pmh-harvester` is available.
1. Run one dry run:

```bash
OAI_BASE_URL=https://example.org/oai?verb=ListRecords uv run oai-pmh-harvester --dry-run
```

1. Run live dry with `OPEN_ACCESS_ONLY` and without to validate both modes
   as needed.

## Post-deploy verification

- Confirm successful completion logs include `harvested/ uploaded / active /`
  `deleted` summary.
- Validate state file `resumptionToken` progresses as expected.
- Re-run with same query window to verify idempotency.

## Rollback

If a run fails repeatedly:

- Inspect last state and restart after clearing/resetting resumption token if needed.
- Re-run with unchanged query parameters and track progress in logs.
