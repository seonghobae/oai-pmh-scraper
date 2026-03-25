# One-Day Delivery Plan

## Goal

Deliver a reliable OAI-PMH harvester iteration that safely resumes
harvesting, handles protocol edge cases, and remains PR-merge-gate ready.

## Scope

- In scope
  - Core harvester behavior (paging, state, parser error handling, OA filtering).
  - Deterministic tests for resumed runs and failure paths.
  - Canonical docs required by repository operating policy.
  - `uv run pytest` green with lint/type-check sanity.
- Out of scope
  - New ingestion sources.
  - UI/reporting stack.
  - Snowflake schema redesign.

## Deliverables for today

1. State-loading and resume-key safeguards are explicit and tested.
2. CLI/runner cleanup is robust on errors.
3. Canonical docs exist in-repo:
   - `docs/engineering/acceptance-criteria.md`
   - `docs/engineering/harness-engineering.md`
   - `docs/workflow/one-day-delivery-plan.md`
   - `docs/workflow/pr-continuity.md`
   - `docs/agents/README.md`
   - `docs/coderabbit/review-commands.md`
   - `docs/security/api-security-checklist.md`
   - `docs/operations/deploy-runbook.md`
4. `ARCHITECTURE.md` reflects behavior-affecting changes.

## Risks and mitigations

- **State mismatch by query parameters**: mismatch silently reusing token.
  - Mitigation: include full harvest query identity in state validity check.
- **Protocol error loops**: `badResumptionToken` leaves stale cursor.
  - Mitigation: clear token on error and persist state.
- **CI/validation gaps**: no canonical docs or tests for edge paths.
  - Mitigation: pre-merge full suite + checklists.
