from __future__ import annotations

import re
import pytest
from typing import Literal

from oai_harvester.models import OaiRecord
from oai_harvester.storage import SnowflakeStorage


class FakeCursor:
    def __init__(
        self,
        *,
        fail_executemany: bool = False,
        session_autocommit: bool = False,
    ) -> None:
        self.fail_executemany = fail_executemany
        self.session_autocommit = session_autocommit
        self.execute_calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self._autocommit_query_seen = False

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        return False

    def execute(self, sql: str, params: object | None = None) -> None:
        self.execute_calls.append((sql, params))
        self._autocommit_query_seen = "CURRENT_SETTING('AUTOCOMMIT')" in sql

    def executemany(self, sql: str, params: list[tuple[object, ...]]) -> None:
        if self.fail_executemany:
            raise RuntimeError("bulk failure")
        self.executemany_calls.append((sql, params))

    def fetchone(self):
        if self._autocommit_query_seen:
            return ("true" if self.session_autocommit else "false",)
        return None

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(
        self,
        *,
        fail_executemany: bool = False,
        session_autocommit: bool = False,
    ) -> None:
        self.cursor_obj = FakeCursor(
            fail_executemany=fail_executemany,
            session_autocommit=session_autocommit,
        )
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

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
        source_url="https://example.org/oai",
        open_access_flags=[True, False],
    )

    assert count == 2
    # connection autocommit check + ensure_table call
    assert len(connection.cursor_obj.execute_calls) == 2
    assert (
        "current_setting('autocommit')"
        in connection.cursor_obj.execute_calls[0][0].lower()
    )
    create_sql, _ = connection.cursor_obj.execute_calls[1]
    assert "primary key (source_url, identifier)" in create_sql.lower()
    # bulk upsert call
    assert len(connection.cursor_obj.executemany_calls) == 1
    sql, params = connection.cursor_obj.executemany_calls[0]
    assert "merge into" in sql.lower()
    assert "on tgt.source_url = src.source_url" in sql.lower()
    assert "and tgt.identifier = src.identifier" in sql.lower()
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

    with pytest.raises(
        ValueError,
        match=re.escape("source_url='https://example.org/oai'"),
    ):
        storage.upsert_records(
            records,
            source_url="https://example.org/oai",
            open_access_flags=[True, False],
        )


def test_storage_rolls_back_on_upsert_failure() -> None:
    connection = FakeConnection(fail_executemany=True)
    storage = _build_storage(connection)
    records = [
        OaiRecord(
            identifier="id-1",
            datestamp="2026-01-01",
            metadata={},
            raw_record_xml="<record id='1'/>",
        )
    ]

    with pytest.raises(RuntimeError, match="bulk failure"):
        storage.upsert_records(
            records,
            source_url="https://example.org/oai",
            open_access_flags=[True],
        )

    assert connection.rollback_count == 1
    assert connection.commit_count == 0


def test_storage_rejects_injected_autocommit_connection() -> None:
    with pytest.raises(ValueError, match="autocommit"):
        _build_storage(FakeConnection(session_autocommit=True))
