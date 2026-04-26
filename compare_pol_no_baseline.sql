from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any
import math
import re
import unicodedata


DEFAULT_SKIP_KEYS = {
    "order",
    "resolution",
    "ref_raw",
    "setting_id",
    "setting_name",
    "setting_raw_name",
}

DIFF_SKIP_KEYS = {
    "_compare_sort_key",
}

NON_FINITE_STRINGS = {
    "nan", "+nan", "-nan",
    "inf", "+inf", "-inf",
    "infinity", "+infinity", "-infinity",
}


def compare_policy_json(
    old_policy_json: Any,
    new_policy_json: Any,
    *,
    skip_keys: set[str] | None = None,
    max_diffs: int = 100,
) -> dict[str, Any]:
    old_norm = normalize_policy_json(old_policy_json, skip_keys=skip_keys)
    new_norm = normalize_policy_json(new_policy_json, skip_keys=skip_keys)

    diffs = diff_json_values(old_norm, new_norm, max_diffs=max_diffs)

    return {
        "equal": len(diffs) == 0,
        "diff_count": len(diffs),
        "diffs": diffs,
    }


def normalize_policy_json(
    value: Any,
    *,
    skip_keys: set[str] | None = None,
    path: str = "",
) -> Any:
    if skip_keys is None:
        skip_keys = DEFAULT_SKIP_KEYS

    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return _normalize_string(value)

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Non-finite float is not allowed: {value!r}")
        return Decimal(str(value))

    if isinstance(value, list):
        normalized_items = [
            normalize_policy_json(item, skip_keys=skip_keys, path=f"{path}[]")
            for item in value
        ]

        if path == "settings" or path.endswith(".settings"):
            _ensure_unique_sort_keys(normalized_items, path)
            return sorted(
                normalized_items,
                key=lambda item: (
                    item.get("_compare_sort_key", "")
                    if isinstance(item, dict)
                    else ""
                ),
            )

        return normalized_items

    if isinstance(value, dict):
        normalized: dict[str, Any] = {}

        # For settings items: use ref_raw only as stable sort/match key.
        # It is not compared as business data.
        if path == "settings[]" or path.endswith(".settings[]"):
            normalized["_compare_sort_key"] = _stable_key(value.get("ref_raw"))

        for key, val in value.items():
            key = str(key)

            if key in skip_keys:
                continue

            child_path = f"{path}.{key}" if path else key
            normalized[key] = normalize_policy_json(
                val,
                skip_keys=skip_keys,
                path=child_path,
            )

        return normalized

    raise TypeError(f"Unsupported JSON value type: {type(value)!r}")


def diff_json_values(
    left: Any,
    right: Any,
    *,
    path: str = "",
    max_diffs: int = 100,
) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    _diff_json_values(left, right, path, diffs, max_diffs)
    return diffs


def _diff_json_values(
    left: Any,
    right: Any,
    path: str,
    diffs: list[dict[str, Any]],
    max_diffs: int,
) -> None:
    if len(diffs) >= max_diffs:
        return

    if type(left) is not type(right):
        diffs.append({
            "path": path,
            "reason": "type_mismatch",
            "left": _safe_value(left),
            "right": _safe_value(right),
        })
        return

    if isinstance(left, dict):
        left_keys = set(left) - DIFF_SKIP_KEYS
        right_keys = set(right) - DIFF_SKIP_KEYS

        for key in sorted(left_keys - right_keys):
            if len(diffs) >= max_diffs:
                return
            diffs.append({
                "path": _join_path(path, key),
                "reason": "missing_on_right",
                "left": _safe_value(left[key]),
                "right": "<missing>",
            })

        for key in sorted(right_keys - left_keys):
            if len(diffs) >= max_diffs:
                return
            diffs.append({
                "path": _join_path(path, key),
                "reason": "missing_on_left",
                "left": "<missing>",
                "right": _safe_value(right[key]),
            })

        for key in sorted(left_keys & right_keys):
            _diff_json_values(
                left[key],
                right[key],
                _join_path(path, key),
                diffs,
                max_diffs,
            )

        return

    if isinstance(left, list):
        min_len = min(len(left), len(right))

        for index in range(min_len):
            _diff_json_values(
                left[index],
                right[index],
                _list_path(path, index),
                diffs,
                max_diffs,
            )

        for index in range(min_len, len(left)):
            if len(diffs) >= max_diffs:
                return
            diffs.append({
                "path": _list_path(path, index),
                "reason": "missing_on_right",
                "left": _safe_value(left[index]),
                "right": "<missing>",
            })

        for index in range(min_len, len(right)):
            if len(diffs) >= max_diffs:
                return
            diffs.append({
                "path": _list_path(path, index),
                "reason": "missing_on_left",
                "left": "<missing>",
                "right": _safe_value(right[index]),
            })

        return

    if left != right:
        diffs.append({
            "path": path,
            "reason": "value_mismatch",
            "left": _safe_value(left),
            "right": _safe_value(right),
        })


def _normalize_string(value: str) -> str | Decimal | None:
    text = unicodedata.normalize("NFC", value).strip()

    if text == "":
        return None

    lowered = text.lower()

    if lowered in NON_FINITE_STRINGS:
        raise ValueError(f"Non-finite numeric string is not allowed: {value!r}")

    try:
        return Decimal(text)
    except InvalidOperation:
        return text


def _stable_key(value: Any) -> str:
    """
    Used only for sorting/matching settings items.

    Lowercase/casefold + remove all whitespace.
    Do not use this for normal value comparison.
    """
    if value is None:
        return ""

    text = unicodedata.normalize("NFC", str(value)).strip().casefold()
    return re.sub(r"\s+", "", text)


def _ensure_unique_sort_keys(items: list[Any], path: str) -> None:
    seen: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            continue

        key = item.get("_compare_sort_key", "")

        if key == "":
            raise ValueError(f"Missing settings compare key at {path}")

        if key in seen:
            raise ValueError(f"Duplicate settings compare key at {path}: {key!r}")

        seen.add(key)


def _join_path(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _list_path(path: str, index: int) -> str:
    return f"{path}[{index}]" if path else f"[{index}]"


def _safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {
            key: _safe_value(val)
            for key, val in value.items()
            if key not in DIFF_SKIP_KEYS
        }

    if isinstance(value, list):
        return [_safe_value(item) for item in value]

    return value


# usage 

result = compare_policy_json(old_policy_data_json, new_policy_data_json)

if result["equal"]:
    # no new version
    pass
else:
    # insert new version
    print(result["diffs"])


# possible high level algorithm

for each staged policy:
    find latest main policy by:
        policy_name
        typeid

    if not found:
        insert staged policy as version 1

    else:
        compare latest_main.policy_data_json vs staged.policy_data_json

        if equal:
            do nothing

        else:
            insert staged policy as version latest_main.policy_version + 1



# tests

import pytest

from your_module import compare_policy_json


def base_policy(settings):
    return {
        "object": {
            "name": "Daniel-Monitor-Policy-001",
            "description": None,
            "featureid": "ENDP_AM_1000",
            "categoryid": "EAM_BehaviorBlock_Policies",
            "typeid": "EAM_BehaviorBlock_Policies",
            "serverid": "HKW20122317",
            "editflag": "0",
        },
        "settings": settings,
        "unresolved_refs": [],
    }


def setting(ref_raw, rule_name, *, order=1, block="1", report="1", note="", extra=None):
    data = {
        "order": order,
        "resolution": "guid",
        "ref_raw": ref_raw,
        "setting_id": "SOME-GENERATED-ID",
        "setting_name": f"{ref_raw}::Settings",
        "setting_raw_name": f"{ref_raw}::Settings",
        "featureid": "ENDP_AM_1000",
        "categoryid": "EAM_BehaviorBlock_Policies",
        "typeid": "EAM_BehaviorBlock_Policies",
        "sections": {
            "APRule": {
                "Block": block,
                "ExecutableCount": "0",
                "Note": note,
                "ParameterCount": "0",
                "Report": report,
                "RuleID": "RULE-ID-001",
                "RuleName": rule_name,
                "RuleType": "Custom",
                "SubRuleCount": "0",
            }
        },
    }

    if extra:
        data["sections"]["APRule"].update(extra)

    return data


def assert_equal(left, right):
    result = compare_policy_json(left, right)
    assert result["equal"], result["diffs"]


def assert_not_equal(left, right):
    result = compare_policy_json(left, right)
    assert not result["equal"]
    assert result["diff_count"] > 0
    return result["diffs"]


def test_same_policy_equal():
    left = base_policy([
        setting("Policy::SettingA", "Disable Safe Mode"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Disable Safe Mode"),
    ])

    assert_equal(left, right)


def test_settings_order_does_not_matter():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    right = base_policy([
        setting("Policy::SettingB", "Rule B"),
        setting("Policy::SettingA", "Rule A"),
    ])

    assert_equal(left, right)


def test_order_and_resolution_are_ignored():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", order=1),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", order=999),
    ])

    right["settings"][0]["resolution"] = "different-resolution"

    assert_equal(left, right)


def test_setting_ids_and_names_are_ignored():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right["settings"][0]["setting_id"] = "DIFFERENT-ID"
    right["settings"][0]["setting_name"] = "Different Name"
    right["settings"][0]["setting_raw_name"] = "Different Raw Name"

    assert_equal(left, right)


def test_ref_raw_used_for_sorting_but_not_compared_directly():
    left = base_policy([
        setting("Policy :: Setting A", "Rule A"),
    ])

    right = base_policy([
        setting("policy::settinga", "Rule A"),
    ])

    assert_equal(left, right)


def test_blank_string_and_whitespace_are_equal():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", note=""),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", note="     "),
    ])

    assert_equal(left, right)


def test_blank_string_and_none_are_equal():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", note=""),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", note=None),
    ])

    assert_equal(left, right)


def test_numeric_string_and_number_are_equal():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", block="1", report="1"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", block=1, report=1),
    ])

    assert_equal(left, right)


def test_decimal_string_and_int_are_equal():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1.00"}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": 1}),
    ])

    assert_equal(left, right)


def test_string_values_are_stripped():
    left = base_policy([
        setting("Policy::SettingA", "  Disable Safe Mode  "),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Disable Safe Mode"),
    ])

    assert_equal(left, right)


def test_unicode_normalization_equal():
    left = base_policy([
        setting("Policy::SettingA", "café"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "cafe\u0301"),
    ])

    assert_equal(left, right)


def test_empty_settings_lists_are_equal():
    left = base_policy([])
    right = base_policy([])

    assert_equal(left, right)


def test_new_setting_in_stage_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_left" for d in diffs)


def test_setting_missing_from_stage_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_right" for d in diffs)


def test_changed_existing_setting_value_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", block="1"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", block="0"),
    ])

    diffs = assert_not_equal(left, right)

    assert any("Block" in d["path"] for d in diffs)


def test_changed_rule_name_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Old Rule Name"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "New Rule Name"),
    ])

    diffs = assert_not_equal(left, right)

    assert any("RuleName" in d["path"] for d in diffs)


def test_added_field_inside_section_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"NewField": "new-value"}),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_left" for d in diffs)


def test_removed_field_inside_section_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"OldField": "old-value"}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_right" for d in diffs)


def test_nested_subrule_value_change_is_difference():
    left = base_policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            extra={
                "SubRule#0_Name": "Windows Update",
                "SubRule#0_Operations": "srv_start srv_startup",
            },
        ),
    ])

    right = base_policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            extra={
                "SubRule#0_Name": "Windows Update",
                "SubRule#0_Operations": "srv_stop",
            },
        ),
    ])

    diffs = assert_not_equal(left, right)

    assert any("SubRule#0_Operations" in d["path"] for d in diffs)


def test_unresolved_refs_empty_list_equal():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    left["unresolved_refs"] = []
    right["unresolved_refs"] = []

    assert_equal(left, right)


def test_unresolved_refs_changed_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    left["unresolved_refs"] = []
    right["unresolved_refs"] = ["missing-ref"]

    diffs = assert_not_equal(left, right)

    assert any("unresolved_refs" in d["path"] for d in diffs)


def test_duplicate_ref_raw_keys_raise_error():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
        setting("policy :: setting a", "Rule A duplicated"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(ValueError, match="Duplicate settings compare key"):
        compare_policy_json(left, right)


def test_missing_ref_raw_raises_error():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    del left["settings"][0]["ref_raw"]

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(ValueError, match="Missing settings compare key"):
        compare_policy_json(left, right)


def test_nan_string_raises_error():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "NaN"}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite numeric string"):
        compare_policy_json(left, right)


def test_infinity_string_raises_error():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "Infinity"}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite numeric string"):
        compare_policy_json(left, right)


def test_float_nan_raises_error():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": float("nan")}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite float"):
        compare_policy_json(left, right)


def test_list_order_inside_business_section_still_matters():
    left = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": ["a", "b"]}),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": ["b", "a"]}),
    ])

    diffs = assert_not_equal(left, right)

    assert any("Values" in d["path"] for d in diffs)


def test_top_level_metadata_change_is_difference_by_default():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right["object"]["serverid"] = "DIFFERENT-SERVER"

    diffs = assert_not_equal(left, right)

    assert any("serverid" in d["path"] for d in diffs)


def test_top_level_object_name_change_is_difference():
    left = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = base_policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right["object"]["name"] = "Different Policy Name"

    diffs = assert_not_equal(left, right)

    assert any("object.name" in d["path"] for d in diffs)