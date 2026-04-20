# phase 2
from __future__ import annotations

from lxml import etree


@dataclass(frozen=True, slots=True)
class ParsedSettingsMaps:
    by_id: dict[str, ParsedPolicySetting]
    by_fallback_key: dict[str, ParsedPolicySetting]
    duplicate_ids: tuple[str, ...]
    duplicate_fallback_keys: tuple[str, ...]


def parse_policy_settings_from_xml_path(xml_path: str) -> ParsedSettingsMaps:
    by_id: dict[str, ParsedPolicySetting] = {}
    by_fallback_key: dict[str, ParsedPolicySetting] = {}
    duplicate_ids: list[str] = []
    duplicate_fallback_keys: list[str] = []

    context = etree.iterparse(
        xml_path,
        events=("end",),
        tag="EPOPolicySettings",
        recover=False,
        huge_tree=True,
    )

    for _, elem in context:
        raw_name = normalize_text(elem.get("name"))
        if not raw_name:
            _clear_element(elem)
            continue

        display_name, guid = parse_name_with_guid(raw_name)
        fallback_key = make_fallback_key(raw_name)

        featureid = normalize_nullable_text(elem.get("featureid"))
        categoryid = normalize_nullable_text(elem.get("categoryid"))
        typeid = normalize_nullable_text(elem.get("typeid"))

        sections: dict[str, dict[str, str]] = {}
        for sec in elem.findall("Section"):
            sec_name = normalize_text(sec.get("name"))
            settings: dict[str, str] = {}
            for setting in sec.findall("Setting"):
                sname = setting.get("name")
                if sname is None:
                    continue
                settings[sname] = normalize_text(setting.get("value"))
            sections[sec_name] = settings

        parsed = ParsedPolicySetting(
            setting_id=guid,
            fallback_key=fallback_key,
            raw_name=raw_name,
            display_name=display_name,
            featureid=featureid,
            categoryid=categoryid,
            typeid=typeid,
            sections=sections,
            xml_blob=etree.tostring(elem, encoding="unicode"),
        )

        if guid:
            if guid in by_id:
                duplicate_ids.append(guid)
            else:
                by_id[guid] = parsed

        if fallback_key in by_fallback_key:
            duplicate_fallback_keys.append(fallback_key)
        else:
            by_fallback_key[fallback_key] = parsed

        _clear_element(elem)

    return ParsedSettingsMaps(
        by_id=by_id,
        by_fallback_key=by_fallback_key,
        duplicate_ids=tuple(duplicate_ids),
        duplicate_fallback_keys=tuple(duplicate_fallback_keys),
    )


# phase 3


from __future__ import annotations

import json
from typing import Any


def assemble_staged_policies(
    *,
    ingest_id: str,
    objects_map: dict[str, ParsedPolicyObject],
    settings_maps: ParsedSettingsMaps,
) -> list[StagedPolicyRow]:
    rows: list[StagedPolicyRow] = []

    for obj in objects_map.values():
        resolved_settings: list[dict[str, Any]] = []
        xml_parts: list[str] = [obj.xml_blob]
        unresolved_refs: list[dict[str, Any]] = []

        for ref in obj.setting_refs:
            setting = None
            resolution = None

            if ref.setting_id:
                setting = settings_maps.by_id.get(ref.setting_id)
                if setting is not None:
                    resolution = "guid"

            if setting is None:
                setting = settings_maps.by_fallback_key.get(ref.fallback_key)
                if setting is not None:
                    resolution = "fallback_key"

            if setting is None:
                unresolved_refs.append(
                    {
                        "order": ref.order,
                        "raw_ref": ref.raw_ref,
                        "display_name": ref.display_name,
                        "setting_id": ref.setting_id,
                        "fallback_key": ref.fallback_key,
                    }
                )
                continue

            resolved_settings.append(
                {
                    "order": ref.order,
                    "resolution": resolution,
                    "ref_raw": ref.raw_ref,
                    "setting_id": setting.setting_id,
                    "setting_name": setting.display_name,
                    "setting_raw_name": setting.raw_name,
                    "featureid": setting.featureid,
                    "categoryid": setting.categoryid,
                    "typeid": setting.typeid,
                    "sections": setting.sections,
                }
            )
            xml_parts.append(setting.xml_blob)

        payload = {
            "object": {
                "name": obj.object_name,
                "description": obj.description,
                "featureid": obj.featureid,
                "categoryid": obj.categoryid,
                "typeid": obj.typeid,
                "serverid": obj.severid,
                "editflag": obj.edtflag,
            },
            "settings": resolved_settings,
            "unresolved_refs": unresolved_refs,
        }

        wrapped_xml = wrap_policy_xml_bundle(
            ingest_id=ingest_id,
            object_name=obj.object_name,
            xml_parts=xml_parts,
        )

        metadata = {
            "linked_settings_count": len(obj.setting_refs),
            "resolved_settings_count": len(resolved_settings),
            "unresolved_settings_count": len(unresolved_refs),
        }

        rows.append(
            StagedPolicyRow(
                ingest_id=ingest_id,
                policy_name=obj.object_name,
                policy_description=obj.description,
                featureid=obj.featureid,
                categoryid=obj.categoryid,
                typeid=obj.typeid,
                serverid=obj.severid,
                editflag=obj.edtflag,
                policy_metadata=json.dumps(metadata, ensure_ascii=False),
                policy_data_json=json.dumps(payload, ensure_ascii=False),
                policy_data_xml=wrapped_xml,
                policy_hash=stable_policy_hash(payload),
            )
        )

    return rows


def wrap_policy_xml_bundle(
    *,
    ingest_id: str,
    object_name: str,
    xml_parts: list[str],
) -> str:
    inner = "".join(xml_parts)
    return (
        f'<PolicyBundle ingest_id="{xml_escape_attr(ingest_id)}" '
        f'policy_name="{xml_escape_attr(object_name)}">'
        f"{inner}"
        f"</PolicyBundle>"
    )


def xml_escape_attr(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# still phase 3 shell and orchestraiton


from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator


def write_chunks_to_tempfile(chunks: Iterator[bytes]) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        for chunk in chunks:
            tmp.write(chunk)
    return tmp_path


def ingest_policies_from_chunks(
    *,
    chunks: Iterator[bytes],
    get_session,
    repository_factory,
    logger,
    ingest_id: str | None = None,
    batch_size: int = 100,
) -> str:
    resolved_ingest_id = ingest_id or make_ingest_id()
    tmp_path = write_chunks_to_tempfile(chunks)

    try:
        logger.info("Starting policy ingest", extra={"ingest_id": resolved_ingest_id})

        objects_map = parse_policy_objects_from_xml_path(tmp_path)
        settings_maps = parse_policy_settings_from_xml_path(tmp_path)

        logger.info(
            "Parsed XML structures",
            extra={
                "ingest_id": resolved_ingest_id,
                "objects_count": len(objects_map),
                "settings_by_id_count": len(settings_maps.by_id),
                "settings_by_fallback_count": len(settings_maps.by_fallback_key),
                "duplicate_setting_ids": len(settings_maps.duplicate_ids),
                "duplicate_fallback_keys": len(settings_maps.duplicate_fallback_keys),
            },
        )

        rows = assemble_staged_policies(
            ingest_id=resolved_ingest_id,
            objects_map=objects_map,
            settings_maps=settings_maps,
        )

        logger.info(
            "Assembled staged policy rows",
            extra={
                "ingest_id": resolved_ingest_id,
                "rows_count": len(rows),
            },
        )

        with get_session() as session:
            repository = repository_factory(session)
            save_staged_policy_rows(
                rows=rows,
                repository=repository,
                logger=logger,
                ingest_id=resolved_ingest_id,
                batch_size=batch_size,
            )

        logger.info("Finished policy ingest", extra={"ingest_id": resolved_ingest_id})
        return resolved_ingest_id

    finally:
        os.remove(tmp_path)


# Repository shape

from __future__ import annotations

from sqlalchemy import text


class StagedPoliciesRepository:
    def __init__(self, session):
        self._session = session

    def insert_staged_policies(self, rows: list[StagedPolicyRow]) -> None:
        if not rows:
            return

        self._session.execute(
            text(
                """
                INSERT INTO dbo.stg_policies (
                    ingest_id,
                    policy_name,
                    policy_description,
                    featureid,
                    categoryid,
                    typeid,
                    serverid,
                    editflag,
                    policy_metadata,
                    policy_hash,
                    policy_data_json,
                    policy_data_xml
                )
                VALUES (
                    :ingest_id,
                    :policy_name,
                    :policy_description,
                    :featureid,
                    :categoryid,
                    :typeid,
                    :serverid,
                    :editflag,
                    :policy_metadata,
                    :policy_hash,
                    :policy_data_json,
                    :policy_data_xml
                )
                """
            ),
            [
                {
                    "ingest_id": row.ingest_id,
                    "policy_name": row.policy_name,
                    "policy_description": row.policy_description,
                    "featureid": row.featureid,
                    "categoryid": row.categoryid,
                    "typeid": row.typeid,
                    "serverid": row.serverid,
                    "editflag": row.editflag,
                    "policy_metadata": row.policy_metadata,
                    "policy_hash": row.policy_hash,
                    "policy_data_json": row.policy_data_json,
                    "policy_data_xml": row.policy_data_xml,
                }
                for row in rows
            ],
        )
        self._session.commit()


# Batch save shell save orchestraiton

def save_staged_policy_rows(
    *,
    rows: list[StagedPolicyRow],
    repository: StagedPoliciesRepository,
    logger,
    ingest_id: str,
    batch_size: int = 100,
) -> None:
    batch: list[StagedPolicyRow] = []

    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            repository.insert_staged_policies(batch)
            logger.info(
                "Inserted batch of staged policies",
                extra={"ingest_id": ingest_id, "batch_size": len(batch)},
            )
            batch.clear()

    if batch:
        repository.insert_staged_policies(batch)
        logger.info(
            "Inserted final batch of staged policies",
            extra={"ingest_id": ingest_id, "batch_size": len(batch)},
        )