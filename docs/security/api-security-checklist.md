# API Security Checklist

## Authentication & access

- Credentials are loaded from environment variables only.
- No API keys or credentials are persisted in state files or logs.
- Network calls include a configurable user agent.

## Transport & protocol

- OAI endpoint and requests parameters are explicit (metadataPrefix/set/from/until/resumptionToken).
- Transport errors are converted to typed exceptions.
- XML protocol errors (`noRecordsMatch`, `badResumptionToken`, etc.) are
  surfaced with structured error codes.

## Input and parser handling

- Malformed XML is explicitly translated to parse errors.
- Corrupt state payloads should be handled by resetting to deterministic
  baseline state.

## Data handling

- State file stores only resumability metadata (no credentials).
- Deleted records are retained in upsert flow for synchronization consistency.
- Logs avoid printing raw secrets.

## Dependency security governance

- Dependency exceptions must be explicit, temporary, and documented in
  `docs/security/dependency-exceptions.md` with exit criteria.
