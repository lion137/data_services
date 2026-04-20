# core helpers
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

GUID_RE = re.compile(
    r"^(?P<name>.*?)\s*\((?P<guid>[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})\)\s*$"
)


def normalize_text(value: str | None) -> str:
    return "" if value is None else value.strip()


def normalize_nullable_text(value: str | None) -> str | None:
    text = normalize_text(value)
    return text or None


def make_fallback_key(raw_text: str) -> str:
    return normalize_text(raw_text)


def parse_name_with_guid(raw_text: str) -> tuple[str, str | None]:
    text = normalize_text(raw_text)
    if not text:
        return "", None

    match = GUID_RE.match(text)
    if not match:
        return text, None

    return normalize_text(match.group("name")), match.group("guid").upper()


def make_ingest_id(now: datetime | None = None) -> str:
    ts = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"{ts}_{suffix}"


def stable_policy_hash(payload: dict[str, Any]) -> bytes:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).digest()


# shell hepler 

def write_chunks_to_tempfile(chunks: Iterator[bytes]) -> str:
    chunks = preprocess_first_chunk(chunks)
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        for ch in chunks:
            tmp.write(ch)
    return tmp_path

# core models

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PolicySettingRef:
    raw_ref: str
    setting_id: str | None
    fallback_key: str
    display_name: str
    order: int


@dataclass(frozen=True, slots=True)
class ParsedPolicyObject:
    object_name: str
    featureid: str | None
    categoryid: str | None
    typeid: str | None
    severid: str | None
    edtflag: str | None
    description: str | None
    setting_refs: tuple[PolicySettingRef, ...]
    xml_blob: str


@dataclass(frozen=True, slots=True)
class ParsedPolicySetting:
    setting_id: str | None
    fallback_key: str
    raw_name: str
    display_name: str
    featureid: str | None
    categoryid: str | None
    typeid: str | None
    sections: dict[str, dict[str, str]]
    xml_blob: str


@dataclass(frozen=True, slots=True)
class StagedPolicyRow:
    ingest_id: str
    policy_name: str
    policy_description: str | None
    featureid: str | None
    categoryid: str | None
    typeid: str | None
    serverid: str | None
    editflag: str | None
    policy_metadata: str | None
    policy_data_json: str
    policy_data_xml: str | None
    policy_hash: bytes

# core for phase 1 ingest poicy objects into inmemory dict

from __future__ import annotations

from collections.abc import Iterator
from lxml import etree


def parse_policy_objects_from_xml_path(xml_path: str) -> dict[str, ParsedPolicyObject]:
    objects: dict[str, ParsedPolicyObject] = {}

    context = etree.iterparse(
        xml_path,
        events=("end",),
        tag="EPOPolicyObject",
        recover=False,
        huge_tree=True,
    )

    for _, elem in context:
        object_name = normalize_text(elem.get("name"))
        if not object_name:
            _clear_element(elem)
            continue

        featureid = normalize_nullable_text(elem.get("featureid"))
        categoryid = normalize_nullable_text(elem.get("categoryid"))
        typeid = normalize_nullable_text(elem.get("typeid"))
        severid = normalize_nullable_text(elem.get("serverid"))
        edtflag = normalize_nullable_text(elem.get("editflag"))

        description_elem = elem.find("description")
        description = None
        if description_elem is not None:
            description = normalize_nullable_text(description_elem.text)

        setting_refs: list[PolicySettingRef] = []
        for order, ref_elem in enumerate(elem.findall("PolicySettings"), start=1):
            raw_ref = normalize_text(ref_elem.text)
            if not raw_ref:
                continue

            display_name, guid = parse_name_with_guid(raw_ref)
            setting_refs.append(
                PolicySettingRef(
                    raw_ref=raw_ref,
                    setting_id=guid,
                    fallback_key=make_fallback_key(raw_ref),
                    display_name=display_name,
                    order=order,
                )
            )

        xml_blob = etree.tostring(elem, encoding="unicode")

        objects[object_name] = ParsedPolicyObject(
            object_name=object_name,
            featureid=featureid,
            categoryid=categoryid,
            typeid=typeid,
            severid=severid,
            edtflag=edtflag,
            description=description,
            setting_refs=tuple(setting_refs),
            xml_blob=xml_blob,
        )

        _clear_element(elem)

    return objects


def _clear_element(elem: etree._Element) -> None:
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]