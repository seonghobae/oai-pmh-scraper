from __future__ import annotations

from dataclasses import dataclass
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class HarvestState:
    source: str
    metadata_prefix: str
    set_spec: str | None
    from_date: str | None
    until_date: str | None
    resumption_token: str | None
    total_records: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "metadata_prefix": self.metadata_prefix,
            "set_spec": self.set_spec,
            "from_date": self.from_date,
            "until_date": self.until_date,
            "resumption_token": self.resumption_token,
            "total_records": self.total_records,
        }


def load_state(
    path: Path,
    source: str,
    metadata_prefix: str,
    set_spec: str | None,
    from_date: str | None,
    until_date: str | None,
) -> HarvestState:
    if not path.exists():
        return HarvestState(
            source=source,
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_date=from_date,
            until_date=until_date,
            resumption_token=None,
            total_records=0,
        )

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, JSONDecodeError):
        return HarvestState(
            source=source,
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_date=from_date,
            until_date=until_date,
            resumption_token=None,
            total_records=0,
        )
    if not isinstance(payload, dict):
        return HarvestState(
            source=source,
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_date=from_date,
            until_date=until_date,
            resumption_token=None,
            total_records=0,
        )

    if (
        payload.get("source") != source
        or payload.get("metadata_prefix") != metadata_prefix
        or payload.get("set_spec") != set_spec
        or payload.get("from_date") != from_date
        or payload.get("until_date") != until_date
    ):
        return HarvestState(
            source=source,
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_date=from_date,
            until_date=until_date,
            resumption_token=None,
            total_records=0,
        )

    total_records = 0
    raw_total_records = payload.get("total_records")
    if isinstance(raw_total_records, int):
        total_records = raw_total_records
    elif isinstance(raw_total_records, str) and raw_total_records.isdigit():
        total_records = int(raw_total_records)

    return HarvestState(
        source=source,
        metadata_prefix=metadata_prefix,
        set_spec=set_spec,
        from_date=from_date,
        until_date=until_date,
        resumption_token=payload.get("resumption_token"),
        total_records=total_records,
    )


def save_state(path: Path, state: HarvestState) -> None:
    path.write_text(
        json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
