from __future__ import annotations

import pytest

from oai_harvester.parser import OaiListRecordsPage, parse_oai_listrecords
from oai_harvester.errors import OAIError, OAINoRecords, OAIProtocolError


def test_parse_records_with_token() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header>
            <identifier>oai:example:1</identifier>
            <datestamp>2026-01-01T00:00:00Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
              <dc:title>title 1</dc:title>
              <dc:rights>Open Access</dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <resumptionToken>next-token</resumptionToken>
      </ListRecords>
    </OAI-PMH>
    """

    page = parse_oai_listrecords(xml)

    assert isinstance(page, OaiListRecordsPage)
    assert page.resumption_token == "next-token"
    assert len(page.records) == 1
    assert page.records[0].identifier == "oai:example:1"
    assert page.records[0].metadata["dc"]["title"] == "title 1"


def test_parse_deleted_record() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header status="deleted">
            <identifier>oai:example:2</identifier>
            <datestamp>2026-01-02T00:00:00Z</datestamp>
          </header>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    page = parse_oai_listrecords(xml)
    assert len(page.records) == 1
    assert page.records[0].deleted is True


def test_no_records_match_raises_specific_exception() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="noRecordsMatch">No records found</error>
    </OAI-PMH>
    """

    with pytest.raises(OAINoRecords, match="No records found"):
        parse_oai_listrecords(xml)


def test_unknown_oai_error_bubbles_up() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="badResumptionToken">Invalid token</error>
    </OAI-PMH>
    """

    with pytest.raises(OAIError):
        parse_oai_listrecords(xml)


def test_bad_resumption_token_is_protocol_error() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="badResumptionToken">Bad token</error>
    </OAI-PMH>
    """

    with pytest.raises(OAIProtocolError, match="Bad token") as exc_info:
        parse_oai_listrecords(xml)
    assert exc_info.value.code == "badResumptionToken"


def test_parse_xml_with_unbound_namespace_prefix() -> None:
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords>
        <record>
          <header><identifier>oai:example:3</identifier><datestamp>2026-01-03T00:00:00Z</datestamp></header>
          <metadata>
            <dc:rights>open access</dc:rights>
          </metadata>
        </record>
      </ListRecords>
    </OAI-PMH>
    """

    page = parse_oai_listrecords(xml)
    assert len(page.records) == 1
    assert page.records[0].identifier == "oai:example:3"
