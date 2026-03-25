from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class OaiRecord:
    identifier: str
    datestamp: str
    metadata: Mapping[str, Any]
    raw_record_xml: str
    deleted: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )

    @property
    def status(self) -> str:
        return "deleted" if self.deleted else "active"
