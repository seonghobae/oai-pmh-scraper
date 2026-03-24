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
OAI_BASE_URL=https://example.org/oai uv run oai-pmh-harvester --dry-run
```

1. Validate both OA modes explicitly:
   - **Dry-run (no writes):** run with `--dry-run` and set
     `OPEN_ACCESS_ONLY=true` when validating OA-only filtering.
   - **Live run (writes enabled):** run without `--dry-run`; toggle
     `OPEN_ACCESS_ONLY` as required by the target ingestion policy.

## Post-deploy verification

- Confirm successful completion logs include `harvested/ uploaded / active /`
  `deleted` summary.
- Validate state file `resumptionToken` progresses as expected.
- Re-run with same query window to verify idempotency.

## Rollback

If a run fails repeatedly:

- Inspect last state and restart after clearing/resetting resumption token if needed.
- Re-run with unchanged query parameters and track progress in logs.
