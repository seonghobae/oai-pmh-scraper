from __future__ import annotations

from dataclasses import dataclass

import requests  # type: ignore[import-untyped]

from .errors import OAITransportError


@dataclass
class OaiRequest:
    verb: str
    metadata_prefix: str | None = None
    set_spec: str | None = None
    from_date: str | None = None
    until_date: str | None = None
    resumption_token: str | None = None


class OaiClient:
    def __init__(
        self,
        base_url: str,
        user_agent: str,
        timeout_seconds: int,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    def _build_params(self, req: OaiRequest) -> dict[str, str]:
        params: dict[str, str] = {"verb": req.verb}
        if req.resumption_token:
            params["resumptionToken"] = req.resumption_token
            return params

        if req.metadata_prefix:
            params["metadataPrefix"] = req.metadata_prefix
        if req.set_spec:
            params["set"] = req.set_spec
        if req.from_date:
            params["from"] = req.from_date
        if req.until_date:
            params["until"] = req.until_date
        return params

    def fetch(self, req: OaiRequest) -> str:
        params = self._build_params(req)
        try:
            response = self._session.get(
                self.base_url, params=params, timeout=self.timeout_seconds
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise OAITransportError("Failed to fetch OAI-PMH response") from exc
        return response.text

    def close(self) -> None:
        self._session.close()

    def list_records(
        self,
        *,
        metadata_prefix: str,
        set_spec: str | None,
        from_date: str | None,
        until_date: str | None,
        resumption_token: str | None,
    ) -> str:
        request = OaiRequest(
            verb="ListRecords",
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            from_date=from_date,
            until_date=until_date,
            resumption_token=resumption_token,
        )
        return self.fetch(request)
