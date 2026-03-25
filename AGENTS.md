# AGENTS.md

## Repository overview

- This repository provides a Python OAI-PMH harvester (`oai-pmh-harvester`) that
  fetches records, persists resumable state, and upserts to Snowflake.
- Canonical repository policy documents live under `docs/**` and are the
  authoritative source for workflow, acceptance, operations, and security rules.

## Setup

- Install dependencies: `uv sync`
- Run CLI help: `uv run oai-pmh-harvester --help`

## Verification commands

- Unit tests: `uv run pytest -q`
- Lint/check by changed filetype:
  - `PYTHONPATH="${OPENCODE_HOME:-$HOME/.config/opencode}"` and then
    `python3 -m scripts.lint_by_filetype --json`

## Delivery and PR policy

- Default working branch is feature/closeout branch, then PR into `main`.
- Keep fixes scoped to the active canonical PR when the branch already maps to
  an open PR.
- Resolve review findings with code/test/doc updates, then verify and push.
- Do not dismiss requested reviews without explicit user instruction.
- When required checks and review gates pass, enable merge through normal
  PR flow.

## Documentation maintenance

- If behavior changes in parser/state/runner/storage, update `ARCHITECTURE.md`.
- Keep these canonical docs current:
  - `docs/engineering/acceptance-criteria.md`
  - `docs/engineering/harness-engineering.md`
  - `docs/workflow/one-day-delivery-plan.md`
  - `docs/workflow/pr-continuity.md`
  - `docs/agents/README.md`
  - `docs/coderabbit/review-commands.md`
  - `docs/security/api-security-checklist.md`
  - `docs/operations/deploy-runbook.md`

## Security

- Never commit secrets or credential values.
- Keep state files, caches, virtualenv artifacts, and generated outputs out of git.
