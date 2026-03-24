from __future__ import annotations

from oai_harvester.client import OaiClient


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")


class FakeSession:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[dict[str, object]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, str], timeout: int):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(self.text)

    def close(self) -> None:
        pass

    def update(self, value: dict[str, str]) -> None:
        self.headers.update(value)


def test_client_build_params_without_resumption_token() -> None:
    session = FakeSession("<OAI-PMH/>")
    client = OaiClient("https://example.org/oai", "ua", 15)
    client._session = session  # type: ignore[attr-defined]

    text = client.list_records(
        metadata_prefix="oai_dc",
        set_spec="test",
        from_date="2026-01-01",
        until_date="2026-02-01",
        resumption_token=None,
    )

    assert text == "<OAI-PMH/>"
    assert session.calls[0]["params"] == {
        "verb": "ListRecords",
        "metadataPrefix": "oai_dc",
        "set": "test",
        "from": "2026-01-01",
        "until": "2026-02-01",
    }


def test_client_uses_resumption_token_when_present() -> None:
    session = FakeSession("<OAI-PMH/>")
    client = OaiClient("https://example.org/oai", "ua", 15)
    client._session = session  # type: ignore[attr-defined]

    client.list_records(
        metadata_prefix="oai_dc",
        set_spec=None,
        from_date=None,
        until_date=None,
        resumption_token="abc123",
    )

    assert session.calls[0]["params"] == {
        "verb": "ListRecords",
        "resumptionToken": "abc123",
    }


def test_client_transport_error() -> None:
    class ErrSession(FakeSession):
        def get(self, url, params, timeout):  # type: ignore[override]
            raise RuntimeError("network")

    client = OaiClient("https://example.org/oai", "ua", 15)
    client._session = ErrSession("<OAI-PMH/>")  # type: ignore[attr-defined]

    from oai_harvester.errors import OAITransportError

    try:
        client.list_records(
            metadata_prefix="oai_dc",
            set_spec=None,
            from_date=None,
            until_date=None,
            resumption_token=None,
        )
    except OAITransportError:
        pass
    else:
        raise AssertionError("Expected OAITransportError")
