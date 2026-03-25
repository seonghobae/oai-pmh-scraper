# Acceptance Criteria

## Project Goals

- The repository must implement an OAI-PMH harvester that can collect records
  from OAI-PMH `ListRecords`, persist state, and upsert into Snowflake.
- Open Access filtering, dedupe, and resume-on-failure behavior must be
  deterministic and test-covered.
- All changes are developed under the configured branch protection and PR workflow.

## Completion Requirements

The project is considered complete only when all items are met:

1. **Functionality**
   - `oai-pmh-harvester` CLI runs with required `OAI_BASE_URL`.
   - Supports resumption token paging and state-based restart.
   - Handles OAI protocol errors, including `noRecordsMatch` and `badResumptionToken`.
   - Can filter records for Open Access and still propagate deletes for sync correctness.
   - Deduplicates records by identifier using latest datestamp prior to upsert.

2. **Reliability**
   - Unit tests cover parser, client, config, state, and runner behavior.
   - `uv run pytest` passes on clean checkout.
   - Error handling for transport and parse failures is explicit and surfaced.

3. **Process / Governance**
   - Canonical docs in this repo are present and current.
   - Changes are committed through PR workflow and guarded by review requirements.

4. **Security / Config**
   - Snowflake credentials are read from environment only.
   - No secrets committed to source control.

5. **Operational Readiness**
   - README includes runbook-level usage and required environment variables.
   - State file behavior and retry/error behavior are documented.
