from __future__ import annotations

import pytest

from oai_harvester.models import OaiRecord


def test_record_metadata_is_immutable_mapping() -> None:
    source_metadata = {"rights": "open access"}
    record = OaiRecord(
        identifier="id-1",
        datestamp="2026-01-01",
        metadata=source_metadata,
        raw_record_xml="<record/>",
    )

    source_metadata["rights"] = "closed"
    assert record.metadata["rights"] == "open access"

    with pytest.raises(TypeError):
        record.metadata["rights"] = "modified"  # type: ignore[index]
