from __future__ import annotations

import argparse
import os

from .client import OaiClient
from .config import HarvesterConfig, load_config
from .runner import Harvester
from .storage import SnowflakeStorage


def _build_storage(config: HarvesterConfig) -> SnowflakeStorage | None:
    if not config.is_snowflake_enabled:
        return None
    assert config.sf_account is not None
    assert config.sf_user is not None
    assert config.sf_password is not None
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
        f"active={result.active_records} "
        f"deleted={result.deleted_records}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="oai-pmh-harvester")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return run_harvest(dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
