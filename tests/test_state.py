from __future__ import annotations

import pytest

from oai_harvester.state import HarvestState, load_state, save_state


def test_state_roundtrip(tmp_path) -> None:
    path = tmp_path / "state.json"
    state = HarvestState(
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec="physics",
        from_date=None,
        until_date=None,
        resumption_token="abc",
        total_records=3,
    )
    save_state(path, state)
    loaded = load_state(
        path, "https://example.org/oai", "oai_dc", "physics", None, None
    )

    assert loaded == state


def test_state_source_mismatch_starts_clean(tmp_path) -> None:
    path = tmp_path / "state.json"
    state = HarvestState(
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        resumption_token="abc",
        total_records=3,
    )
    save_state(path, state)

    loaded = load_state(path, "https://other.org/oai", "oai_dc", None, None, None)
    assert loaded.resumption_token is None
    assert loaded.total_records == 0


def test_state_query_shape_mismatch_starts_clean(tmp_path) -> None:
    path = tmp_path / "state.json"
    state = HarvestState(
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec="physics",
        from_date=None,
        until_date=None,
        resumption_token="abc",
        total_records=3,
    )
    save_state(path, state)

    loaded = load_state(
        path,
        "https://example.org/oai",
        "mods",
        "math",
        "2025-01-01",
        "2025-01-31",
    )
    assert loaded.resumption_token is None
    assert loaded.total_records == 0


def test_state_invalid_resumption_token_type_starts_clean(tmp_path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        '{"source":"https://example.org/oai","metadata_prefix":"oai_dc","set_spec":null,'
        '"from_date":null,"until_date":null,"resumption_token":123,"total_records":0}',
        encoding="utf-8",
    )

    loaded = load_state(path, "https://example.org/oai", "oai_dc", None, None, None)

    assert loaded.resumption_token is None
    assert loaded.total_records == 0


def test_state_corrupt_json_starts_clean(tmp_path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{this is not json", encoding="utf-8")

    loaded = load_state(path, "https://example.org/oai", "oai_dc", None, None, None)
    assert loaded.resumption_token is None
    assert loaded.total_records == 0


def test_state_invalid_total_records_values_start_clean(tmp_path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        '{"source":"https://example.org/oai","metadata_prefix":"oai_dc","set_spec":null,'
        '"from_date":null,"until_date":null,"resumption_token":null,"total_records":-5}',
        encoding="utf-8",
    )

    loaded = load_state(path, "https://example.org/oai", "oai_dc", None, None, None)
    assert loaded.resumption_token is None
    assert loaded.total_records == 0

    path.write_text(
        '{"source":"https://example.org/oai","metadata_prefix":"oai_dc","set_spec":null,'
        '"from_date":null,"until_date":null,"resumption_token":null,"total_records":true}',
        encoding="utf-8",
    )

    loaded_bool = load_state(
        path, "https://example.org/oai", "oai_dc", None, None, None
    )
    assert loaded_bool.resumption_token is None
    assert loaded_bool.total_records == 0


@pytest.mark.parametrize(
    ("metadata_prefix", "from_date", "until_date"),
    [
        ("mods", None, None),
        ("oai_dc", "2025-01-01", None),
        ("oai_dc", None, "2025-01-31"),
    ],
)
def test_state_other_query_shape_mismatch_starts_clean(
    tmp_path,
    metadata_prefix: str,
    from_date: str | None,
    until_date: str | None,
) -> None:
    path = tmp_path / "state.json"
    state = HarvestState(
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec="physics",
        from_date=None,
        until_date=None,
        resumption_token="abc",
        total_records=3,
    )
    save_state(path, state)

    loaded = load_state(
        path,
        "https://example.org/oai",
        metadata_prefix,
        "physics",
        from_date,
        until_date,
    )

    assert loaded.resumption_token is None
    assert loaded.total_records == 0
