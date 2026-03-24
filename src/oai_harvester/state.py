from __future__ import annotations

from dataclasses import dataclass
import json
import os
import tempfile
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


def _default_state(
    source: str,
    metadata_prefix: str,
    set_spec: str | None,
    from_date: str | None,
    until_date: str | None,
) -> HarvestState:
    return HarvestState(
        source=source,
        metadata_prefix=metadata_prefix,
        set_spec=set_spec,
        from_date=from_date,
        until_date=until_date,
        resumption_token=None,
        total_records=0,
    )


def _coerce_resumption_token(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def load_state(
    path: Path,
    source: str,
    metadata_prefix: str,
    set_spec: str | None,
    from_date: str | None,
    until_date: str | None,
) -> HarvestState:
    if not path.exists():
        return _default_state(source, metadata_prefix, set_spec, from_date, until_date)

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, JSONDecodeError):
        return _default_state(source, metadata_prefix, set_spec, from_date, until_date)
    if not isinstance(payload, dict):
        return _default_state(source, metadata_prefix, set_spec, from_date, until_date)

    if (
        payload.get("source") != source
        or payload.get("metadata_prefix") != metadata_prefix
        or payload.get("set_spec") != set_spec
        or payload.get("from_date") != from_date
        or payload.get("until_date") != until_date
    ):
        return _default_state(source, metadata_prefix, set_spec, from_date, until_date)

    total_records = 0
    raw_total_records = payload.get("total_records")
    if isinstance(raw_total_records, int):
        total_records = raw_total_records
    elif isinstance(raw_total_records, str) and raw_total_records.isdigit():
        total_records = int(raw_total_records)

    resumption_token = _coerce_resumption_token(payload.get("resumption_token"))

    return HarvestState(
        source=source,
        metadata_prefix=metadata_prefix,
        set_spec=set_spec,
        from_date=from_date,
        until_date=until_date,
        resumption_token=resumption_token,
        total_records=total_records,
    )


def save_state(path: Path, state: HarvestState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, suffix=".tmp", encoding="utf-8", delete=False
        ) as file:
            tmp_path = file.name
            content = json.dumps(state.to_dict(), ensure_ascii=False, indent=2)
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and os.path.exists(tmp_path):
            os.remove(tmp_path)
