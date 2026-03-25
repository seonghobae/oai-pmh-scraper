from __future__ import annotations

import pytest
from pathlib import Path

from oai_harvester import cli
from oai_harvester.config import HarvesterConfig


def _base_config(state_file: Path) -> HarvesterConfig:
    return HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=state_file,
        open_access_only=False,
        open_access_terms=("open access",),
        batch_size=0,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )


def test_cli_closes_resources_on_harvest_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class FakeClient:
        def close(self) -> None:
            events.append("client.close")

    class FakeHarvester:
        def __init__(
            self,
            *args,
            **kwargs,
        ) -> None:
            pass

        def run(self, *, dry_run: bool = False) -> object:
            raise RuntimeError("failure")

    class FakeStorage:
        def close(self) -> None:
            events.append("storage.close")

    monkeypatch.setattr(
        cli, "load_config", lambda env: _base_config(tmp_path / "state.json")
    )
    monkeypatch.setattr(cli, "OaiClient", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(cli, "_build_storage", lambda config: FakeStorage())
    monkeypatch.setattr(cli, "Harvester", FakeHarvester)

    with pytest.raises(RuntimeError):
        cli.run_harvest(dry_run=False)

    assert events == ["storage.close", "client.close"]


def test_cli_closes_client_even_if_storage_close_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class FakeClient:
        def close(self) -> None:
            events.append("client.close")

    class FakeHarvester:
        def __init__(
            self,
            *args,
            **kwargs,
        ) -> None:
            pass

        def run(self, *, dry_run: bool = False) -> object:
            raise RuntimeError("harvest failure")

    class FakeStorage:
        def close(self) -> None:
            events.append("storage.close")
            raise RuntimeError("storage close failure")

    monkeypatch.setattr(
        cli, "load_config", lambda env: _base_config(tmp_path / "state.json")
    )
    monkeypatch.setattr(cli, "OaiClient", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(cli, "_build_storage", lambda config: FakeStorage())
    monkeypatch.setattr(cli, "Harvester", FakeHarvester)

    with pytest.raises(RuntimeError, match="harvest failure"):
        cli.run_harvest(dry_run=False)

    assert events == ["storage.close", "client.close"]


def test_cli_closes_client_when_storage_build_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class FakeClient:
        def close(self) -> None:
            events.append("client.close")

    monkeypatch.setattr(
        cli, "load_config", lambda env: _base_config(tmp_path / "state.json")
    )
    monkeypatch.setattr(cli, "OaiClient", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(
        cli,
        "_build_storage",
        lambda config: (_ for _ in ()).throw(RuntimeError("storage init failure")),
    )

    with pytest.raises(RuntimeError, match="storage init failure"):
        cli.run_harvest(dry_run=False)

    assert events == ["client.close"]
