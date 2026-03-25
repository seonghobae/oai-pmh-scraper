# Dependency Security Exceptions

## pyOpenSSL 26 compatibility exception (temporary)

- Introduced: 2026-03-25
- Scope: Dependabot `uv` ecosystem updates
- Affected dependency: `pyOpenSSL` / `pyopenssl`
- Current constraint blocker:
  `snowflake-connector-python==4.3.0` requires `pyOpenSSL>=24.0.0,<26.0.0`.

### Why this exists

Dependabot security update runs attempted to force `pyOpenSSL>=26.0.0`, which is
currently unsatisfiable with the Snowflake connector constraint. This produced
failing update workflows despite an otherwise healthy repository state.

### Compensating controls

- Keep Dependabot enabled for the `uv` ecosystem.
- Keep runtime dependency pins and lockfile (`uv.lock`) under version control.
- Re-check upstream Snowflake connector dependency metadata on each dependency
  maintenance cycle.

## Dependabot alert triage (2026-03-25)

The following alerts were reviewed against actual runtime usage and upstream
package constraints.

| Alert | Dependency | Severity | Disposition |
| --- | --- | --- | --- |
| GHSA-5pwr-322w-8jr4 | pyOpenSSL | high | temporary tolerable risk |
| GHSA-vp96-hxj8-p424 | pyOpenSSL | low | temporary tolerable risk |
| GHSA-5239-wwwm-4pmq | Pygments | low | temporary tolerable risk |

Rationale summary:

- Included CVEs: CVE-2026-27459, CVE-2026-27448, CVE-2026-4539.
- `pyOpenSSL>=26` fixes are blocked by
  `snowflake-connector-python==4.3.0` requiring `pyOpenSSL<26`.
- The Pygments advisory currently reports no first patched version.

### Triage guardrails

- Revisit all temporary dismissals on each dependency maintenance cycle.
- Re-open and remediate immediately when upstream publishes a compatible patched
  release.
- Keep this file synchronized with Dependabot alert state changes.

### Exit criteria

1. Snowflake connector publishes a release compatible with `pyOpenSSL>=26`.
2. Remove the temporary ignore entries from `.github/dependabot.yml`.
3. Update lockfile and run full verification (`pytest` + lint/type checks).
