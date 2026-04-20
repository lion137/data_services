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