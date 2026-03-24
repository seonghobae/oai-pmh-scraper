# Agents README

This repository is maintained primarily by agents with defined scopes.

## Core agent use cases

- `general`: quick repository audits, documentation checks, and multi-step planning.
- `review`: code review passes, finding quality and security regression risks.
- `requesting-code-review`: pre-merge verification and review coordination.
- `testing`-focused agents: targeted test updates for parser/state/runner behavior.

## Invocation guidance

- For non-trivial refactors, use multiple agents in parallel (implementation + verification).
- Prefer small, isolated tasks per agent to reduce merge conflicts.
- Avoid asking agents to mutate each other’s local uncommitted work.

## Scope boundaries

- **Implementation agents** change production code and tests.
- **Review agents** only audit and recommend.
- **Documentation agents** only edit canonical docs and policy files.

## Safety constraints

- Do not add secrets in any file.
- Keep generated/venv artifacts out of version control.
- Do not drop required branch/PR continuity behavior (open commit -> PR ->
  review -> merge flow).
