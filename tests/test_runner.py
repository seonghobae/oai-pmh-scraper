from __future__ import annotations

import pytest

from oai_harvester.config import HarvesterConfig
from oai_harvester.models import OaiRecord
from oai_harvester.runner import (
    Harvester,
    _filter_storage_records,
    _iter_unique,
    is_open_access,
)
from oai_harvester.errors import OAIProtocolError


class FakeClient:
    def __init__(self, xml_pages: list[str]) -> None:
        self._xml_pages = list(xml_pages)
        self.params: list[dict[str, str | None]] = []

    def list_records(
        self,
        *,
        metadata_prefix: str,
        set_spec: str | None,
        from_date: str | None,
        until_date: str | None,
        resumption_token: str | None,
    ) -> str:
        self.params.append(
            {
                "metadata_prefix": metadata_prefix,
                "set_spec": set_spec,
                "from_date": from_date,
                "until_date": until_date,
                "resumption_token": resumption_token,
            }
        )
        return self._xml_pages.pop(0)


class FakeStorage:
    def __init__(self) -> None:
        self.calls: list[tuple[list[OaiRecord], list[bool]]] = []

    def upsert_records(
        self, records: list[OaiRecord], source_url: str, open_access_flags: list[bool]
    ) -> int:
        self.calls.append((list(records), list(open_access_flags)))
        return len(records)


def test_open_access_detection_default_terms() -> None:
    record = OaiRecord(
        identifier="id1",
        datestamp="2026-01-01",
        metadata={"rights": "This is Open Access paper."},
        raw_record_xml="<record/>",
    )
    assert is_open_access(record, ("open access", "cc-by")) is True


def test_iter_unique_prefers_valid_latest_datestamp() -> None:
    newer_valid = OaiRecord(
        identifier="id1",
        datestamp="2026-01-10",
        metadata={},
        raw_record_xml="<record/>",
    )
    invalid_date = OaiRecord(
        identifier="id1",
        datestamp="not-a-date",
        metadata={},
        raw_record_xml="<record/>",
    )

    deduped = _iter_unique([newer_valid, invalid_date])

    assert len(deduped) == 1
    assert deduped[0].datestamp == "2026-01-10"


def test_filter_storage_records_raises_on_mismatched_lengths() -> None:
    record = OaiRecord(
        identifier="id1",
        datestamp="2026-01-10",
        metadata={},
        raw_record_xml="<record/>",
    )

    with pytest.raises(ValueError, match="shorter than"):
        _filter_storage_records(
            [record],
            [],
            open_access_only=True,
        )


def test_runner_preserves_deleted_records_when_open_access_filter_enabled(
    tmp_path,
) -> None:
    page = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header status="deleted">
            <identifier>id-deleted</identifier>
            <datestamp>2026-01-01</datestamp>
          </header>
        </record>
        <record>
          <header><identifier>id-open</identifier><datestamp>2026-01-02</datestamp></header>
          <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
              <dc:rights>open access</dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header><identifier>id-closed</identifier><datestamp>2026-01-03</datestamp></header>
          <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
              <dc:rights>closed access</dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    client = FakeClient([page])
    storage = FakeStorage()
    cfg = HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=tmp_path / "state.json",
        open_access_only=True,
        open_access_terms=("open access",),
        batch_size=0,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )

    result = Harvester(cfg, client=client, storage=storage).run(dry_run=False)

    assert len(storage.calls) == 1
    stored_records = storage.calls[0][0]
    assert len(stored_records) == 2
    identifiers = {record.identifier for record in stored_records}
    assert identifiers == {"id-deleted", "id-open"}
    assert result.uploaded_records == 2
    assert result.active_records == 2
    assert result.deleted_records == 1


def test_runner_resets_state_and_raises_on_bad_resumption_token(tmp_path) -> None:
    bad_token_page = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="badResumptionToken">Invalid token</error>
    </OAI-PMH>
    """

    client = FakeClient([bad_token_page])
    storage = FakeStorage()
    cfg = HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=tmp_path / "state.json",
        open_access_only=False,
        open_access_terms=("open access",),
        batch_size=0,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )

    # seed state with bad token
    from oai_harvester.state import save_state
    from oai_harvester.state import HarvestState

    seeded_state = HarvestState(
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        resumption_token="bad-token",
        total_records=5,
    )
    save_state(tmp_path / "state.json", seeded_state)

    with pytest.raises(OAIProtocolError) as exc_info:
        Harvester(cfg, client=client, storage=storage).run(dry_run=False)
    assert exc_info.value.code == "badResumptionToken"

    from oai_harvester.state import load_state

    resumed = load_state(
        tmp_path / "state.json",
        source="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
    )
    assert resumed.resumption_token is None
    assert resumed.total_records == 0


def test_runner_with_resumption_token(tmp_path) -> None:
    page1 = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header><identifier>id-1</identifier><datestamp>2026-01-01</datestamp></header>
          <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:rights>open access</dc:rights></oai_dc:dc></metadata>
        </record>
        <resumptionToken>cursor-2</resumptionToken>
      </ListRecords>
    </OAI-PMH>
    """
    page2 = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header><identifier>id-2</identifier><datestamp>2026-01-02</datestamp></header>
          <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:rights>Closed</dc:rights></oai_dc:dc></metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    client = FakeClient([page1, page2])
    storage = FakeStorage()
    cfg = HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=tmp_path / "state.json",
        open_access_only=True,
        open_access_terms=("open access",),
        batch_size=0,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )

    result = Harvester(cfg, client=client, storage=storage).run(dry_run=False)

    # first request starts without token
    assert client.params[0]["resumption_token"] is None
    # second request continues with token
    assert client.params[1]["resumption_token"] == "cursor-2"

    # open_access_only True: first kept second dropped
    assert storage.calls[0][0][0].identifier == "id-1"
    assert len(storage.calls[0][0]) == 1
    assert len(storage.calls) == 1

    assert result.total_records == 2
    assert result.uploaded_records == 1
    assert result.active_records == 2
    assert result.deleted_records == 0


def test_runner_continues_when_page_is_empty_but_token_exists(tmp_path) -> None:
    page1 = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <resumptionToken>cursor-2</resumptionToken>
      </ListRecords>
    </OAI-PMH>
    """
    page2 = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header><identifier>id-2</identifier><datestamp>2026-01-02</datestamp></header>
          <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:rights>open access</dc:rights></oai_dc:dc></metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    client = FakeClient([page1, page2])
    storage = FakeStorage()
    cfg = HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=tmp_path / "state.json",
        open_access_only=False,
        open_access_terms=("open access",),
        batch_size=0,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )

    result = Harvester(cfg, client=client, storage=storage).run(dry_run=False)

    assert len(client.params) == 2
    assert client.params[1]["resumption_token"] == "cursor-2"
    assert result.total_records == 1
    assert result.uploaded_records == 1


def test_runner_respects_batch_size_for_storage_writes(tmp_path) -> None:
    page = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header><identifier>id-1</identifier><datestamp>2026-01-01</datestamp></header>
          <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:rights>open access</dc:rights></oai_dc:dc></metadata>
        </record>
        <record>
          <header><identifier>id-2</identifier><datestamp>2026-01-02</datestamp></header>
          <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:rights>open access</dc:rights></oai_dc:dc></metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    cfg = HarvesterConfig(
        base_url="https://example.org/oai",
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        state_file=tmp_path / "state.json",
        open_access_only=False,
        open_access_terms=("open access",),
        batch_size=1,
        timeout_seconds=30,
        user_agent="test",
        sf_account="acc",
        sf_user="u",
        sf_password="p",
    )

    client = FakeClient([page])
    storage = FakeStorage()
    result = Harvester(cfg, client=client, storage=storage).run(dry_run=False)

    assert len(storage.calls) == 2
    assert len(storage.calls[0][0]) == 1
    assert len(storage.calls[1][0]) == 1
    assert result.uploaded_records == 2
    assert result.active_records == 2
    assert result.deleted_records == 0
