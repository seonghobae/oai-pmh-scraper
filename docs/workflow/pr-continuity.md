# PR Continuity

## Canonical PR policy

- Prefer one canonical PR per stack branch sequence when multiple changes are interdependent.
- Before merging, recompute PR stack status for all open related PRs.
- If blocked PR has duplicates or stale stacks, rebase/retarget as needed.

## Merge path rules

- Required order: implementation PRs -> review resolution -> PR gate pass
  -> mergeability checks -> auto-merge enable.
- If `CHANGES_REQUESTED`, continue preparing follow-up fixes in the same
  branch where possible, or split non-critical follow-up PRs.
- Do not stop service work solely because one PR is blocked; continue with
  mergeable PRs in the same queue where appropriate.

## Recovery and hygiene

- Use `git status`, open PR list, and open-check visibility before deciding blockers.
- If PR state changes during merge attempts, recalculate paths before making
  new assumptions.
