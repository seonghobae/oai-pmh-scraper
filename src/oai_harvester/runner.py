from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from datetime import datetime
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
    active_records: int
    deleted_records: int


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


class _StorageProtocol(Protocol):
    def upsert_records(
        self,
        records: list[OaiRecord],
        *,
        source_url: str,
        open_access_flags: list[bool],
    ) -> int: ...


_DATESTAMP_FORMATS = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d")


def _parse_datestamp(datestamp: str) -> datetime:
    for fmt in _DATESTAMP_FORMATS:
        try:
            return datetime.strptime(datestamp, fmt)
        except ValueError:
            continue
    return datetime.min


def _iter_unique(records: list[OaiRecord]) -> list[OaiRecord]:
    by_identifier: dict[str, OaiRecord] = {}
    for record in records:
        prev = by_identifier.get(record.identifier)
        if prev is None:
            by_identifier[record.identifier] = record
            continue
        if _parse_datestamp(prev.datestamp) < _parse_datestamp(record.datestamp):
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


def _classify_records(
    records: list[OaiRecord], terms: tuple[str, ...]
) -> tuple[list[bool], int, int]:
    open_flags: list[bool] = []
    active_records = 0
    deleted_records = 0

    for record in records:
        if record.deleted:
            open_flags.append(False)
            deleted_records += 1
            continue

        is_open = is_open_access(record, terms)
        open_flags.append(is_open)
        active_records += 1

    return open_flags, active_records, deleted_records


def _filter_storage_records(
    records: list[OaiRecord], open_access_flags: list[bool], *, open_access_only: bool
) -> tuple[list[OaiRecord], list[bool]]:
    if not open_access_only:
        return list(records), list(open_access_flags)

    filtered_records: list[OaiRecord] = []
    filtered_flags: list[bool] = []
    for record, is_open in zip(records, open_access_flags, strict=True):
        if record.deleted or is_open:
            filtered_records.append(record)
            filtered_flags.append(is_open)
    return filtered_records, filtered_flags


class Harvester:
    def __init__(
        self,
        config: HarvesterConfig,
        client: _OaiClientProtocol,
        storage: _StorageProtocol | None,
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

    def _write(
        self,
        records: list[OaiRecord],
        open_access_flags: list[bool],
        source_url: str,
        dry_run: bool,
    ) -> int:
        if not records:
            return 0
        if dry_run or self.storage is None:
            return 0

        uploaded = 0
        for batch_records, batch_flags in _chunk_records(
            records,
            open_access_flags,
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
        active_records = 0
        deleted_records = 0

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
                        total_records=0,
                    )
                    self._save_state(state)
                raise

            records = _iter_unique(page.records)
            (
                record_open_flags,
                page_active_records,
                page_deleted_records,
            ) = _classify_records(records, self.config.open_access_terms)
            storage_records, storage_flags = _filter_storage_records(
                records,
                record_open_flags,
                open_access_only=self.config.open_access_only,
            )
            uploaded += self._write(
                storage_records,
                storage_flags,
                self.config.base_url,
                dry_run=dry_run,
            )
            active_records += page_active_records
            deleted_records += page_deleted_records

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
            active_records=active_records,
            deleted_records=deleted_records,
        )
