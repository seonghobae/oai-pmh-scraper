from __future__ import annotations

import argparse
import os

from .client import OaiClient
from .config import load_config
from .runner import Harvester
from .storage import SnowflakeStorage


def _build_storage(config) -> SnowflakeStorage | None:
    if not config.is_snowflake_enabled:
        return None
    return SnowflakeStorage(
        account=config.sf_account,
        user=config.sf_user,
        password=config.sf_password,
        role=config.sf_role,
        warehouse=config.sf_warehouse,
        database=config.sf_database,
        schema=config.sf_schema,
        table=config.sf_table,
    )


def run_harvest(*, dry_run: bool = False) -> int:
    config = load_config(os.environ)
    client = OaiClient(
        base_url=config.base_url,
        user_agent=config.user_agent,
        timeout_seconds=config.timeout_seconds,
    )
    storage = _build_storage(config)

    try:
        harvester = Harvester(config=config, client=client, storage=storage)
        result = harvester.run(dry_run=dry_run)
    finally:
        if storage is not None:
            storage.close()
        client.close()

    print(
        f"harvested={result.total_records} uploaded={result.uploaded_records} "
        f"active={result.open_records} deleted={result.closed_records}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="oai-pmh-harvester")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return run_harvest(dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
