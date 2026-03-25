from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import re
from typing import Any
from xml.etree import ElementTree as ET
from defusedxml.ElementTree import (  # type: ignore
    DTDForbidden,
    EntitiesForbidden,
    ExternalReferenceForbidden,
    ParseError,
    fromstring as safe_fromstring,
)

from .errors import OAINoRecords, OAIParseError, OAIProtocolError
from .models import OaiRecord

_NS = "{http://www.openarchives.org/OAI/2.0/}"
_TAG_PREFIX_PATTERN = re.compile(r"</?([A-Za-z_][\w.-]*):[A-Za-z_][\w.-]*")
_ATTR_PREFIX_PATTERN = re.compile(
    r"\s(?!(?:xml|xmlns)\b)([A-Za-z_][\w.-]*):[A-Za-z_][\w.-]*\s*="
)
_DECLARED_PREFIX_PATTERN = re.compile(
    r"\s+xmlns:([A-Za-z_][\w.-]*)=(\"[^\"]*\"|'[^']*')"
)
_ROOT_START_TAG_PATTERN = re.compile(
    r"<([A-Za-z_][\w.-]*(?::[A-Za-z_][\w.-]*)?)(\s[^<>]*?)?>"
)


@dataclass(frozen=True)
class OaiListRecordsPage:
    records: list[OaiRecord]
    resumption_token: str | None


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _inject_missing_prefix_declarations(xml_text: str) -> str:
    used_prefixes = {match.group(1) for match in _TAG_PREFIX_PATTERN.finditer(xml_text)}
    used_prefixes.update(
        match.group(1) for match in _ATTR_PREFIX_PATTERN.finditer(xml_text)
    )
    declared_prefixes = {
        match.group(1) for match in _DECLARED_PREFIX_PATTERN.finditer(xml_text)
    }
    missing_prefixes = sorted(used_prefixes - declared_prefixes)
    if not missing_prefixes:
        return xml_text

    root_match = _ROOT_START_TAG_PATTERN.search(xml_text)
    if root_match is None:
        return xml_text

    declarations = "".join(
        f' xmlns:{prefix}="urn:auto-normalized:{prefix}"' for prefix in missing_prefixes
    )
    insert_at = root_match.end() - 1
    return f"{xml_text[:insert_at]}{declarations}{xml_text[insert_at:]}"


def _normalize_xml(xml_text: str) -> str:
    normalized_input = _inject_missing_prefix_declarations(xml_text)
    root = safe_fromstring(normalized_input)
    return ET.tostring(root, encoding="unicode")


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
        root = safe_fromstring(xml_text)
    except (
        DTDForbidden,
        EntitiesForbidden,
        ExternalReferenceForbidden,
        ParseError,
    ) as exc:
        if "unbound prefix" in str(exc):
            try:
                xml_text = _normalize_xml(xml_text)
                root = safe_fromstring(xml_text)
            except (
                DTDForbidden,
                EntitiesForbidden,
                ExternalReferenceForbidden,
                ParseError,
            ) as nested_exc:
                raise OAIParseError("Invalid OAI-PMH XML") from nested_exc

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
            metadata_payload = _to_json(metadata_parent)
            if isinstance(metadata_payload, dict):
                metadata = metadata_payload

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
