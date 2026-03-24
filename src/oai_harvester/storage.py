from __future__ import annotations

import json
from datetime import datetime, timezone

import snowflake.connector  # type: ignore

from .models import OaiRecord


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
        self.database = database
        self.schema = schema
        self.table = table
        if connection is None:
            self.connection = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                role=role,
                warehouse=warehouse,
            )
        else:
            self.connection = connection

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
        count = 0
        with self.connection.cursor() as cursor:
            for record, open_access in zip(records, open_access_flags):
                cursor.execute(
                    sql,
                    [
                        record.identifier,
                        record.status,
                        record.datestamp,
                        json.dumps(record.metadata, ensure_ascii=False),
                        record.raw_record_xml,
                        bool(open_access),
                        source_url,
                        now,
                    ],
                )
                count += 1
        self.connection.commit()
        return count

    def close(self) -> None:
        self.connection.close()
