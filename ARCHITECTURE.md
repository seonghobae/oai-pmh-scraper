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
- Deduplication compares parsed datestamps (`%Y-%m-%dT%H:%M:%SZ` / `%Y-%m-%d`)
  and treats unparseable values as the lowest priority.
- CLI teardown always attempts both `storage.close()` and `client.close()` so one
  close failure cannot skip the other.
- Snowflake upserts are sent in batches with `executemany` (single transaction
  per batch write), rather than one network roundtrip per record.
- Storage writes enforce transaction safety: connections are non-autocommit,
  exceptions trigger rollback, and commit happens only on successful batch write.
- Runner continues pagination based on `resumptionToken` even when a page has
  zero records, and repeated-token detection now resets persisted state then
  raises `badResumptionToken` to avoid carrying a stuck cursor across runs.
- `OaiRecord.metadata` is stored as an immutable mapping copy to prevent
  accidental mutation after record creation.
- Parser namespace normalization injects missing prefix declarations from both
  element tags and attribute QNames (e.g., `xsi:schemaLocation`) before retrying
  parse.
- Snowflake record identity is scoped by `(source_url, identifier)` in both
  schema definition and `MERGE` key to prevent cross-source overwrite.
- Injected storage connections validate session autocommit through
  `CURRENT_SETTING('AUTOCOMMIT')` when available.

## Change impact rule

Any future behavior-affecting change to parser protocol handling, state
validity, filtering, or resumption semantics should:

- update this doc,
- add/adjust tests,
- and update canonical docs as required by policy.
