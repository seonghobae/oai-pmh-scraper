# Deploy Runbook

## Pre-deploy checks

- Confirm `uv run pytest -q` passes on clean working tree.
- Confirm environment variables for target mode:
  - **Required:** `OAI_BASE_URL`
  - **Optional (default `oai_dc`):** `OAI_METADATA_PREFIX`
  - **Optional Snowflake (must be set together if used):**
    `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
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

- Confirm successful completion logs include
  `harvested=X uploaded=Y active=Z deleted=W`.
- Validate state file `resumption_token` progresses as expected.
- Re-run with same query window to verify idempotency.

## Rollback

If a run fails repeatedly:

- If resumption state appears broken, use this minimal reset procedure:
  1. Stop the running harvester process/service.
  2. Locate the configured state file (`OAI_STATE_FILE`, default
     `.oai_harvest_state.json`).
  3. Back up the file, then either delete it or set
     `resumption_token` to `null` in JSON.
  4. Restart the harvester and verify a fresh cursor progression from logs.
- Re-run with unchanged query parameters and track progress in logs.
