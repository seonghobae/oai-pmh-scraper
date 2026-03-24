from __future__ import annotations

import json
from datetime import datetime, timezone
import re

import snowflake.connector  # type: ignore

from .models import OaiRecord

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(value: str, label: str) -> str:
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return f'"{value.replace(chr(34), chr(34) + chr(34))}"'


class SnowflakeStorage:
    def __init__(
        self,
        *,
        account: str,
        user: str,
        password: str,
        role: str | None = None,
        warehouse: str | None = None,
        database: str = "HARMONIA",
        schema: str = "PUBLIC",
        table: str = "PAPERS",
        connection=None,
    ) -> None:
        self.database = _safe_identifier(database, "database")
        self.schema = _safe_identifier(schema, "schema")
        self.table = _safe_identifier(table, "table")
        if connection is None:
            self.connection = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                role=role,
                warehouse=warehouse,
                autocommit=False,
            )
        else:
            self.connection = connection

        autocommit = getattr(self.connection, "autocommit", None)
        if isinstance(autocommit, bool) and autocommit:
            raise ValueError("Injected connection must have autocommit disabled")

    @property
    def full_table(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def ensure_table(self) -> None:
        sql = f"""
        create table if not exists {self.full_table} (
            identifier string primary key,
            status string not null,
            datestamp string,
            metadata variant,
            raw_record_xml string,
            open_access boolean,
            source_url string,
            harvested_at timestamp_ntz
        )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)

    def upsert_records(
        self, records: list[OaiRecord], source_url: str, open_access_flags: list[bool]
    ) -> int:
        if not records:
            return 0
        if len(records) != len(open_access_flags):
            raise ValueError(
                "records/open_access_flags length mismatch for "
                f"source_url={source_url!r}: {len(records)} != {len(open_access_flags)}"
            )

        self.ensure_table()
        sql = f"""
        merge into {self.full_table} as tgt
        using (
            select
                %s as identifier,
                %s as status,
                %s as datestamp,
                parse_json(%s) as metadata,
                %s as raw_record_xml,
                %s as open_access,
                %s as source_url,
                %s as harvested_at
        ) as src
        on tgt.identifier = src.identifier
        when matched then
            update set
                status = src.status,
                datestamp = src.datestamp,
                metadata = src.metadata,
                raw_record_xml = src.raw_record_xml,
                open_access = src.open_access,
                source_url = src.source_url,
                harvested_at = src.harvested_at
        when not matched then
            insert (identifier, status, datestamp, metadata, raw_record_xml, open_access, source_url, harvested_at)
            values (src.identifier, src.status, src.datestamp, src.metadata, src.raw_record_xml, src.open_access, src.source_url, src.harvested_at)
        """

        now = datetime.now(timezone.utc)
        params = [
            (
                record.identifier,
                record.status,
                record.datestamp,
                json.dumps(dict(record.metadata), ensure_ascii=False),
                record.raw_record_xml,
                bool(open_access),
                source_url,
                now,
            )
            for record, open_access in zip(records, open_access_flags, strict=True)
        ]

        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(sql, params)
            self.connection.commit()
        except Exception:
            rollback = getattr(self.connection, "rollback", None)
            if callable(rollback):
                rollback()
            raise
        return len(params)

    def close(self) -> None:
        self.connection.close()
