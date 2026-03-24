# Architecture

## System overview

`oai-pmh-harvester` is a small CLI pipeline:

1. `cli` loads `HarvesterConfig` from environment.
2. `Harvester` (`runner`) requests OAI-PMH `ListRecords` pages from `OaiClient`.
3. `parser` extracts records and resumption token, including protocol errors.
4. Runner dedupes by identifier (latest datestamp), applies OA filtering
   rules, and uploads via `SnowflakeStorage`.
5. Harvest state (`source`, query shape, token, totals) is persisted to resume safely.

## Behavioral decisions

- Deleted records are always forwarded to storage even in OA-only mode to
  maintain tombstone visibility.
- `badResumptionToken` clears saved token before failing fast, preventing
  endless token loop.
- State identity includes source and query shape (`metadata_prefix`, `set_spec`,
  `from_date`, `until_date`).

## Change impact rule

Any future behavior-affecting change to parser protocol handling, state
validity, filtering, or resumption semantics should:

- update this doc,
- add/adjust tests,
- and update canonical docs as required by policy.
