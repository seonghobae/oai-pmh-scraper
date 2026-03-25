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

- Keep Dependabot enabled for both `uv` and GitHub Actions ecosystems.
- Keep runtime dependency pins and lockfile (`uv.lock`) under version control.
- Re-check upstream Snowflake connector dependency metadata on each dependency
  maintenance cycle.

### Exit criteria

1. Snowflake connector publishes a release compatible with `pyOpenSSL>=26`.
2. Remove the temporary ignore entries from `.github/dependabot.yml`.
3. Update lockfile and run full verification (`pytest` + lint/type checks).
