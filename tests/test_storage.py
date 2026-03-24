from __future__ import annotations

import pytest
from typing import Literal

from oai_harvester.models import OaiRecord
from oai_harvester.storage import SnowflakeStorage


class FakeCursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def execute(self, sql: str, params: object | None = None) -> None:
        self.execute_calls.append((sql, params))

    def executemany(self, sql: str, params: list[tuple[object, ...]]) -> None:
        self.executemany_calls.append((sql, params))


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


def _build_storage(connection: FakeConnection) -> SnowflakeStorage:
    return SnowflakeStorage(
        account="acc",
        user="user",
        password="pw",
        database="HARMONIA",
        schema="PUBLIC",
        table="PAPERS",
        connection=connection,
    )


def test_storage_uses_executemany_for_upserts() -> None:
    connection = FakeConnection()
    storage = _build_storage(connection)
    records = [
        OaiRecord(
            identifier="id-1",
            datestamp="2026-01-01",
            metadata={"rights": "open"},
            raw_record_xml="<record id='1'/>",
        ),
        OaiRecord(
            identifier="id-2",
            datestamp="2026-01-02",
            metadata={"rights": "closed"},
            raw_record_xml="<record id='2'/>",
        ),
    ]

    count = storage.upsert_records(
        records,
        "https://example.org/oai",
        [True, False],
    )

    assert count == 2
    # ensure_table call
    assert len(connection.cursor_obj.execute_calls) == 1
    # bulk upsert call
    assert len(connection.cursor_obj.executemany_calls) == 1
    sql, params = connection.cursor_obj.executemany_calls[0]
    assert "merge into" in sql.lower()
    assert len(params) == 2
    assert connection.commit_count == 1


def test_storage_rejects_mismatched_record_and_flag_lengths() -> None:
    connection = FakeConnection()
    storage = _build_storage(connection)
    records = [
        OaiRecord(
            identifier="id-1",
            datestamp="2026-01-01",
            metadata={},
            raw_record_xml="<record id='1'/>",
        )
    ]

    with pytest.raises(ValueError, match="source_url='https://example.org/oai'"):
        storage.upsert_records(records, "https://example.org/oai", [True, False])
