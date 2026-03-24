from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from typing import Protocol

from .config import HarvesterConfig
from .errors import OAIProtocolError, OAINoRecords
from .models import OaiRecord
from .parser import OaiListRecordsPage, parse_oai_listrecords
from .state import HarvestState, load_state, save_state


@dataclass
class HarvestResult:
    total_records: int
    uploaded_records: int
    open_records: int
    closed_records: int


class _OaiClientProtocol(Protocol):
    def list_records(
        self,
        *,
        metadata_prefix: str,
        set_spec: str | None,
        from_date: str | None,
        until_date: str | None,
        resumption_token: str | None,
    ) -> str: ...


def _iter_unique(records: list[OaiRecord]) -> list[OaiRecord]:
    by_identifier: dict[str, OaiRecord] = {}
    for record in records:
        prev = by_identifier.get(record.identifier)
        if prev is None:
            by_identifier[record.identifier] = record
            continue
        if prev.datestamp < record.datestamp:
            by_identifier[record.identifier] = record
    return list(by_identifier.values())


def _chunk_records(
    records: list[OaiRecord], flags: list[bool], batch_size: int
) -> Iterable[tuple[list[OaiRecord], list[bool]]]:
    if batch_size <= 0:
        yield records, flags
        return

    for idx in range(0, len(records), batch_size):
        yield records[idx : idx + batch_size], flags[idx : idx + batch_size]


def _normalize_terms(terms: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(term.lower().strip() for term in terms if term.strip())


def _iter_text_values(value: object) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_text_values(item)
        return
    if isinstance(value, dict):
        for nested in value.values():
            yield from _iter_text_values(nested)


def is_open_access(record: OaiRecord, terms: tuple[str, ...]) -> bool:
    normalized = _normalize_terms(terms)
    if not normalized:
        return True

    candidates: list[str] = []
    candidates.extend(_iter_text_values(record.metadata.get("rights")))
    candidates.extend(_iter_text_values(record.metadata.get("license")))
    candidates.extend(_iter_text_values(record.metadata.get("dc")))
    if not candidates:
        candidates.extend(_iter_text_values(record.metadata))

    for candidate in candidates:
        lower = candidate.lower()
        if any(term in lower for term in normalized):
            return True

    raw = record.raw_record_xml.lower()
    return any(term in raw for term in normalized)


class Harvester:
    def __init__(
        self, config: HarvesterConfig, client: _OaiClientProtocol, storage
    ) -> None:
        self.config = config
        self.client = client
        self.storage = storage

    def _load_state(self) -> HarvestState:
        return load_state(
            self.config.state_file,
            source=self.config.base_url,
            metadata_prefix=self.config.metadata_prefix,
            set_spec=self.config.set_spec,
            from_date=self.config.from_date,
            until_date=self.config.until_date,
        )

    def _save_state(self, state: HarvestState) -> None:
        save_state(self.config.state_file, state)

    def _write(self, records: list[OaiRecord], source_url: str, dry_run: bool) -> int:
        if not records:
            return 0

        if self.config.open_access_only:
            filtered_records: list[OaiRecord] = []
            open_flags: list[bool] = []

            for record in records:
                is_open = is_open_access(record, self.config.open_access_terms)
                if record.deleted:
                    filtered_records.append(record)
                    open_flags.append(False)
                    continue
                if is_open:
                    filtered_records.append(record)
                    open_flags.append(True)

            records = filtered_records
        else:
            open_flags = [
                is_open_access(record, self.config.open_access_terms)
                for record in records
            ]

        if dry_run or self.storage is None:
            return 0

        uploaded = 0
        for batch_records, batch_flags in _chunk_records(
            records,
            open_flags,
            self.config.batch_size,
        ):
            uploaded += self.storage.upsert_records(
                batch_records,
                source_url=source_url,
                open_access_flags=batch_flags,
            )
        return uploaded

    def run(self, *, dry_run: bool = False) -> HarvestResult:
        state = self._load_state()
        token = state.resumption_token
        uploaded = 0
        active = 0
        deleted = 0

        while True:
            xml = self.client.list_records(
                metadata_prefix=self.config.metadata_prefix,
                set_spec=self.config.set_spec,
                from_date=self.config.from_date,
                until_date=self.config.until_date,
                resumption_token=token,
            )

            try:
                page = parse_oai_listrecords(xml)
            except OAINoRecords:
                page = OaiListRecordsPage(records=[], resumption_token=None)
            except OAIProtocolError as error:
                if error.code == "badResumptionToken":
                    state = HarvestState(
                        source=self.config.base_url,
                        metadata_prefix=self.config.metadata_prefix,
                        set_spec=self.config.set_spec,
                        from_date=self.config.from_date,
                        until_date=self.config.until_date,
                        resumption_token=None,
                        total_records=state.total_records,
                    )
                    self._save_state(state)
                raise

            records = _iter_unique(page.records)
            uploaded += self._write(records, self.config.base_url, dry_run=dry_run)
            active += len([record for record in records if not record.deleted])
            deleted += len([record for record in records if record.deleted])

            token = page.resumption_token
            state = HarvestState(
                source=self.config.base_url,
                metadata_prefix=self.config.metadata_prefix,
                set_spec=self.config.set_spec,
                from_date=self.config.from_date,
                until_date=self.config.until_date,
                resumption_token=token,
                total_records=state.total_records + len(records),
            )
            self._save_state(state)

            if not token:
                break

            if not page.records:
                break

        return HarvestResult(
            total_records=state.total_records,
            uploaded_records=uploaded,
            open_records=active,
            closed_records=deleted,
        )
