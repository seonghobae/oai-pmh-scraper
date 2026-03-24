from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re
from typing import Any
from xml.etree import ElementTree as ET

from .errors import OAINoRecords, OAIParseError, OAIProtocolError
from .models import OaiRecord

_NS = "{http://www.openarchives.org/OAI/2.0/}"


@dataclass(frozen=True)
class OaiListRecordsPage:
    records: list[OaiRecord]
    resumption_token: str | None


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _normalize_xml(xml_text: str) -> str:
    no_prefixed_namespaces = re.sub(
        r"\s+xmlns:[A-Za-z_][\w.-]*=(\"[^\"]*\"|'[^']*')",
        "",
        xml_text,
    )
    return re.sub(r"<(/?)[A-Za-z_][\w.-]*:", r"<\1", no_prefixed_namespaces)


def _to_json(elem: ET.Element) -> Any:
    children = list(elem)
    if not children:
        return _clean_text(elem.text)

    payload: dict[str, Any] = {}
    for child in children:
        key = child.tag.split("}")[-1]
        value = _to_json(child)
        if key in payload:
            existing = payload[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                payload[key] = [existing, value]
        else:
            payload[key] = value
    return payload


def _iter_records(root: ET.Element) -> Iterable[ET.Element]:
    return root.findall(f".//{_NS}record")


def parse_oai_listrecords(xml_text: str) -> OaiListRecordsPage:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        if "unbound prefix" in str(exc):
            try:
                xml_text = _normalize_xml(xml_text)
                root = ET.fromstring(xml_text)
            except ET.ParseError:
                raise OAIParseError("Invalid OAI-PMH XML") from exc
            return _parse_root(root)
        raise OAIParseError("Invalid OAI-PMH XML") from exc

    return _parse_root(root)


def _parse_root(root: ET.Element) -> OaiListRecordsPage:
    error_element = root.find(f"./{_NS}error")
    if error_element is not None:
        error_code = error_element.get("code")
        message = _clean_text(error_element.text) or "OAI-PMH error"
        if error_code == "noRecordsMatch":
            raise OAINoRecords(message)
        raise OAIProtocolError(error_code or "unknown", message)

    records: list[OaiRecord] = []
    for record in _iter_records(root):
        header = record.find(f"{_NS}header")
        if header is None:
            continue

        identifier = _clean_text(header.findtext(f"{_NS}identifier"))
        datestamp = _clean_text(header.findtext(f"{_NS}datestamp"))
        if not identifier or not datestamp:
            continue

        deleted = header.get("status") == "deleted"
        metadata_parent = record.find(f"{_NS}metadata")
        metadata: dict[str, Any] = {}
        if metadata_parent is not None:
            for child in list(metadata_parent):
                child_key = child.tag.split("}")[-1]
                child_value = _to_json(child)
                if child_key in metadata:
                    existing = metadata[child_key]
                    if isinstance(existing, list):
                        existing.append(child_value)
                    else:
                        metadata[child_key] = [existing, child_value]
                else:
                    metadata[child_key] = child_value

        records.append(
            OaiRecord(
                identifier=identifier,
                datestamp=datestamp,
                metadata=metadata,
                raw_record_xml=ET.tostring(record, encoding="unicode"),
                deleted=deleted,
            )
        )

    token_elem = root.find(f".//{_NS}resumptionToken")
    token = _clean_text(token_elem.text if token_elem is not None else None)
    return OaiListRecordsPage(records=records, resumption_token=token)
