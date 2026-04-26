from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any
import json
import math
import re
import unicodedata


# CHANGES:
# We compare only policy_data_json["settings"].
# Top-level "object" and "unresolved_refs" are ignored completely.

DEFAULT_SKIP_KEYS = {
    "order",
    "resolution",
    "ref_raw",
    "setting_id",
    "setting_name",
    "setting_raw_name",
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
    """
    Compare only Trellix policy settings.

    Ignored:
    - top-level object metadata
    - unresolved_refs
    - setting order/resolution/ref_raw/setting IDs/names

    Used for matching settings:
    - normalized ref_raw
    """

    old_settings = _extract_settings(old_policy_json)
    new_settings = _extract_settings(new_policy_json)

    old_norm = {
        "settings": normalize_settings_list(old_settings, skip_keys=skip_keys)
    }
    new_norm = {
        "settings": normalize_settings_list(new_settings, skip_keys=skip_keys)
    }

    diffs = diff_json_values(old_norm, new_norm, max_diffs=max_diffs)

    return {
        "equal": len(diffs) == 0,
        "diff_count": len(diffs),
        "diffs": diffs,
    }


def _extract_settings(policy_json: Any) -> list[Any]:
    """
    CHANGES:
    Accept either:
    - full policy_data_json dict containing "settings"
    - direct settings list
    - JSON string representing either of the above
    """

    if isinstance(policy_json, str):
        policy_json = json.loads(policy_json)

    if isinstance(policy_json, list):
        return policy_json

    if isinstance(policy_json, dict):
        settings = policy_json.get("settings")

        if settings is None:
            raise ValueError("policy_data_json does not contain 'settings'")

        if not isinstance(settings, list):
            raise TypeError("'settings' must be a list")

        return settings

    raise TypeError(
        f"policy_data_json must be dict, list, or JSON string, got {type(policy_json)!r}"
    )


def normalize_settings_list(
    settings: list[Any],
    *,
    skip_keys: set[str] | None = None,
) -> dict[str, Any]:
    """
    CHANGES:
    Settings are converted to a dict by stable ref_raw key.

    This is safer than list sorting because added/removed settings
    do not shift indexes and create misleading diffs.
    """

    if skip_keys is None:
        skip_keys = DEFAULT_SKIP_KEYS

    result: dict[str, Any] = {}

    for index, setting in enumerate(settings):
        if not isinstance(setting, dict):
            raise TypeError(f"settings[{index}] must be a dict")

        compare_key = _stable_key(setting.get("ref_raw"))

        if compare_key == "":
            raise ValueError(f"Missing ref_raw for settings[{index}]")

        if compare_key in result:
            raise ValueError(
                f"Duplicate settings compare key from ref_raw at settings[{index}]: "
                f"{compare_key!r}"
            )

        result[compare_key] = normalize_json_value(setting, skip_keys=skip_keys)

    return result


def normalize_json_value(
    value: Any,
    *,
    skip_keys: set[str],
) -> Any:
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
        return [
            normalize_json_value(item, skip_keys=skip_keys)
            for item in value
        ]

    if isinstance(value, dict):
        normalized: dict[str, Any] = {}

        for key, val in value.items():
            key = str(key)

            if key in skip_keys:
                continue

            normalized[key] = normalize_json_value(val, skip_keys=skip_keys)

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
        left_keys = set(left)
        right_keys = set(right)

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
    Used only for matching settings by ref_raw.
    Lowercase/casefold + remove all whitespace.
    """

    if value is None:
        return ""

    text = unicodedata.normalize("NFC", str(value)).strip().casefold()
    return re.sub(r"\s+", "", text)


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
        }

    if isinstance(value, list):
        return [_safe_value(item) for item in value]

    return value

# tests

# tests/core/trellix/test_comparison.py

import pytest

from core.trellix.comparison import compare_policy_json


def policy(settings, *, object_override=None, unresolved_refs=None):
    data = {
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
        "unresolved_refs": [] if unresolved_refs is None else unresolved_refs,
    }

    if object_override:
        data["object"].update(object_override)

    return data


def setting(
    ref_raw,
    rule_name,
    *,
    order=1,
    resolution="guid",
    block="1",
    report="1",
    note="",
    setting_id="SETTING-ID-001",
    setting_name=None,
    setting_raw_name=None,
    extra=None,
):
    setting_name = setting_name or f"{ref_raw}::Settings"
    setting_raw_name = setting_raw_name or f"{ref_raw}::Settings"

    data = {
        "order": order,
        "resolution": resolution,
        "ref_raw": ref_raw,
        "setting_id": setting_id,
        "setting_name": setting_name,
        "setting_raw_name": setting_raw_name,
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


def test_same_settings_are_equal():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    assert_equal(left, right)


def test_compares_only_settings_and_ignores_object_metadata():
    left = policy(
        [setting("Policy::SettingA", "Rule A")],
        object_override={
            "name": "Old Name",
            "serverid": "OLD-SERVER",
            "editflag": "0",
            "description": "old description",
        },
    )

    right = policy(
        [setting("Policy::SettingA", "Rule A")],
        object_override={
            "name": "New Name",
            "serverid": "NEW-SERVER",
            "editflag": "999",
            "description": "new description",
        },
    )

    assert_equal(left, right)


def test_unresolved_refs_are_ignored():
    left = policy(
        [setting("Policy::SettingA", "Rule A")],
        unresolved_refs=[],
    )

    right = policy(
        [setting("Policy::SettingA", "Rule A")],
        unresolved_refs=["missing-ref-1", "missing-ref-2"],
    )

    assert_equal(left, right)


def test_settings_order_does_not_matter():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
        setting("Policy::SettingC", "Rule C"),
    ])

    right = policy([
        setting("Policy::SettingC", "Rule C"),
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    assert_equal(left, right)


def test_order_and_resolution_are_ignored():
    left = policy([
        setting("Policy::SettingA", "Rule A", order=1, resolution="guid"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", order=999, resolution="different"),
    ])

    assert_equal(left, right)


def test_setting_identity_fields_are_ignored_as_values():
    left = policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            setting_id="OLD-ID",
            setting_name="Old Name",
            setting_raw_name="Old Raw Name",
        ),
    ])

    right = policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            setting_id="NEW-ID",
            setting_name="New Name",
            setting_raw_name="New Raw Name",
        ),
    ])

    assert_equal(left, right)


def test_ref_raw_matching_key_is_case_and_whitespace_insensitive():
    left = policy([
        setting("Policy :: Setting A", "Rule A"),
    ])

    right = policy([
        setting("policy::settinga", "Rule A"),
    ])

    assert_equal(left, right)


def test_empty_settings_lists_are_equal():
    assert_equal(policy([]), policy([]))


def test_direct_settings_lists_can_be_compared_without_policy_wrapper():
    left = [
        setting("Policy::SettingA", "Rule A"),
    ]

    right = [
        setting("Policy::SettingA", "Rule A"),
    ]

    assert_equal(left, right)


def test_json_string_input_is_supported():
    import json

    left = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = json.dumps(policy([
        setting("Policy::SettingA", "Rule A"),
    ]))

    assert_equal(left, right)


def test_blank_string_and_whitespace_are_equal():
    left = policy([
        setting("Policy::SettingA", "Rule A", note=""),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", note="     "),
    ])

    assert_equal(left, right)


def test_blank_string_and_none_are_equal():
    left = policy([
        setting("Policy::SettingA", "Rule A", note=""),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", note=None),
    ])

    assert_equal(left, right)


def test_string_values_are_stripped():
    left = policy([
        setting("Policy::SettingA", "  Rule A  "),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    assert_equal(left, right)


def test_unicode_normalization_equal():
    left = policy([
        setting("Policy::SettingA", "café"),
    ])

    right = policy([
        setting("Policy::SettingA", "cafe\u0301"),
    ])

    assert_equal(left, right)


def test_numeric_string_and_int_are_equal():
    left = policy([
        setting("Policy::SettingA", "Rule A", block="1", report="1"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", block=1, report=1),
    ])

    assert_equal(left, right)


def test_decimal_string_and_int_are_equal():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1.00"}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": 1}),
    ])

    assert_equal(left, right)


def test_new_setting_in_stage_is_difference():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_left" for d in diffs)
    assert any("settings.policy::settingb" in d["path"] for d in diffs)


def test_setting_missing_from_stage_is_difference():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
        setting("Policy::SettingB", "Rule B"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_right" for d in diffs)
    assert any("settings.policy::settingb" in d["path"] for d in diffs)


def test_changed_existing_setting_value_is_difference():
    left = policy([
        setting("Policy::SettingA", "Rule A", block="1"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", block="0"),
    ])

    diffs = assert_not_equal(left, right)

    assert any("Block" in d["path"] for d in diffs)


def test_changed_rule_name_is_difference():
    left = policy([
        setting("Policy::SettingA", "Old Rule Name"),
    ])

    right = policy([
        setting("Policy::SettingA", "New Rule Name"),
    ])

    diffs = assert_not_equal(left, right)

    assert any("RuleName" in d["path"] for d in diffs)


def test_added_field_inside_existing_setting_is_difference():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"NewField": "new-value"}),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_left" for d in diffs)
    assert any("NewField" in d["path"] for d in diffs)


def test_removed_field_inside_existing_setting_is_difference():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"OldField": "old-value"}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    diffs = assert_not_equal(left, right)

    assert any(d["reason"] == "missing_on_right" for d in diffs)
    assert any("OldField" in d["path"] for d in diffs)


def test_nested_subrule_value_change_is_difference():
    left = policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            extra={
                "SubRule#0_Name": "Windows Update",
                "SubRule#0_Operations": "srv_start srv_startup",
            },
        ),
    ])

    right = policy([
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


def test_list_order_inside_setting_still_matters():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": ["a", "b"]}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": ["b", "a"]}),
    ])

    diffs = assert_not_equal(left, right)

    assert any("Values" in d["path"] for d in diffs)


def test_empty_list_and_missing_list_are_different_inside_setting():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": []}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    diffs = assert_not_equal(left, right)

    assert any("Values" in d["path"] for d in diffs)


def test_empty_list_and_empty_list_are_equal_inside_setting():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": []}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Values": []}),
    ])

    assert_equal(left, right)


def test_duplicate_ref_raw_keys_raise_error():
    left = policy([
        setting("Policy::SettingA", "Rule A"),
        setting("policy :: setting a", "Rule A duplicated"),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(ValueError, match="Duplicate settings compare key"):
        compare_policy_json(left, right)


def test_missing_ref_raw_raises_error():
    left_setting = setting("Policy::SettingA", "Rule A")
    del left_setting["ref_raw"]

    left = policy([left_setting])

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(ValueError, match="Missing ref_raw"):
        compare_policy_json(left, right)


def test_policy_without_settings_raises_error():
    left = {
        "object": {"name": "Policy A"},
        "unresolved_refs": [],
    }

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(ValueError, match="does not contain 'settings'"):
        compare_policy_json(left, right)


def test_settings_must_be_list():
    left = {
        "settings": {"not": "a list"},
    }

    right = policy([
        setting("Policy::SettingA", "Rule A"),
    ])

    with pytest.raises(TypeError, match="'settings' must be a list"):
        compare_policy_json(left, right)


def test_setting_item_must_be_dict():
    left = policy([
        "not-a-dict-setting",
    ])

    right = policy([])

    with pytest.raises(TypeError, match="settings\\[0\\] must be a dict"):
        compare_policy_json(left, right)


def test_nan_string_raises_error():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "NaN"}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite numeric string"):
        compare_policy_json(left, right)


def test_infinity_string_raises_error():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "Infinity"}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite numeric string"):
        compare_policy_json(left, right)


def test_float_nan_raises_error():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": float("nan")}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite float"):
        compare_policy_json(left, right)


def test_float_infinity_raises_error():
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": float("inf")}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"Threshold": "1"}),
    ])

    with pytest.raises(ValueError, match="Non-finite float"):
        compare_policy_json(left, right)


def test_max_diffs_limits_result_size():
    left = policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            extra={
                "A": "1",
                "B": "2",
                "C": "3",
            },
        ),
    ])

    right = policy([
        setting(
            "Policy::SettingA",
            "Rule A",
            extra={
                "A": "10",
                "B": "20",
                "C": "30",
            },
        ),
    ])

    result = compare_policy_json(left, right, max_diffs=2)

    assert not result["equal"]
    assert result["diff_count"] == 2


def test_numeric_identifier_currently_compares_as_number_warning_case():
    """
    Current behavior:
    "001" and 1 are treated as equal because numeric strings become Decimal.

    This may be dangerous for IDs/codes.
    Keep this test visible so we remember to switch to field-aware
    numeric normalization if Trellix has numeric-looking identifiers.
    """
    left = policy([
        setting("Policy::SettingA", "Rule A", extra={"SomeCode": "001"}),
    ])

    right = policy([
        setting("Policy::SettingA", "Rule A", extra={"SomeCode": 1}),
    ])

    assert_equal(left, right)