from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OaiRecord:
    identifier: str
    datestamp: str
    metadata: dict[str, Any]
    raw_record_xml: str
    deleted: bool = False

    @property
    def status(self) -> str:
        return "deleted" if self.deleted else "active"
