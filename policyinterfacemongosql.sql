'''
server_code/services/trellix/policy_rows/
    __init__.py
    models.py
    common.py
    oas.py
    ports.py
    service.py
    sql_repositories.py
    mongo_repositories.py
    bootstrap.py
'''


-- policy_rows/models.py 
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class SourcePolicyRecord:
    source_policy_id: str
    policy_type: str
    policy_name_full: str
    payload_json: Optional[dict[str, Any]]
    payload_xml: Optional[str]


@dataclass(frozen=True)
class ParsedPolicyName:
    policy_base_name: str
    policy_full_name: str
    version_label: Optional[str]


@dataclass(frozen=True)
class PolicySettingRow:
    source_policy_id: str
    policy_type: str
    policy_base_name: str
    policy_full_name: str
    version_label: Optional[str]
    section: str
    subcategory: str
    setting_key: str
    setting_name: str
    raw_value: str
    display_value: Optional[str]


-- policy_rows/ports.py

from __future__ import annotations

from typing import Protocol, Optional

from .models import PolicySettingRow, SourcePolicyRecord


class SourcePolicyRepository(Protocol):
    def list_policies(
        self,
        *,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        ...


class PolicySettingsRowRepository(Protocol):
    def replace_rows_for_source_policy(
        self,
        *,
        source_policy_id: str,
        rows: list[PolicySettingRow],
    ) -> None:
        ...


-- policy_rows/common.py

from __future__ import annotations

from typing import Any
from xml.etree import ElementTree as ET

from .models import ParsedPolicyName


def parse_policy_name(full_name: str) -> ParsedPolicyName:
    normalized = full_name.strip()

    if "::" in normalized:
        left, right = normalized.split("::", 1)
        return ParsedPolicyName(
            policy_base_name=left.strip(),
            policy_full_name=normalized,
            version_label=right.strip() or None,
        )

    return ParsedPolicyName(
        policy_base_name=normalized,
        policy_full_name=normalized,
        version_label=None,
    )


def stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def flatten_policy_json_sections(payload_json: dict[str, Any]) -> list[tuple[str, str, str]]:
    sections = payload_json.get("sections", {})
    result: list[tuple[str, str, str]] = []

    for section_name, section_settings in sections.items():
        if not isinstance(section_settings, dict):
            continue

        for setting_key, raw_value in section_settings.items():
            result.append((section_name, setting_key, stringify(raw_value)))

    return result


def flatten_policy_xml_sections(payload_xml: str) -> list[tuple[str, str, str]]:
    root = ET.fromstring(payload_xml)
    result: list[tuple[str, str, str]] = []

    for section_el in root.findall("./Section"):
        section_name = section_el.attrib.get("name", "").strip()

        for setting_el in section_el.findall("./Setting"):
            setting_key = setting_el.attrib.get("name", "").strip()
            raw_value = setting_el.attrib.get("value", "")
            result.append((section_name, setting_key, raw_value))

    return result


-- policy_rows/oas.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .common import (
    flatten_policy_json_sections,
    flatten_policy_xml_sections,
    parse_policy_name,
)
from .models import PolicySettingRow, SourcePolicyRecord


Translator = Callable[[str], str]


def enabled_disabled(value: str) -> str:
    if value == "1":
        return "Enabled"
    if value == "0":
        return "Disabled"
    return value


def passthrough(value: str) -> str:
    return value


def mapped(mapping: dict[str, str]) -> Translator:
    def _translate(value: str) -> str:
        return mapping.get(value, value)
    return _translate


@dataclass(frozen=True)
class OASRule:
    subcategory: str
    setting_name: str
    translator: Translator


OAS_RULES: dict[str, OASRule] = {
    "bShowAlerts": OASRule(
        subcategory="Threat Detection User Messaging",
        setting_name="Display the On-Access Scan window to users when a threat is detected",
        translator=enabled_disabled,
    ),
    "szDialogMessage": OASRule(
        subcategory="Threat Detection User Messaging",
        setting_name="Threat Detection Message",
        translator=passthrough,
    ),
    "scanUsingAMSIHooks": OASRule(
        subcategory="Antimalware Scan Interface (Windows only)",
        setting_name="Enable AMSI (provides enhanced script scanning) (Windows only)",
        translator=enabled_disabled,
    ),
    "GTISensitivityLevel": OASRule(
        subcategory="Trellix GTI",
        setting_name="Enable Trellix GTI",
        translator=enabled_disabled,
    ),
    "scriptScanEnabled": OASRule(
        subcategory="ScriptScan (Windows only)",
        setting_name="Enable ScriptScan",
        translator=enabled_disabled,
    ),
    "dwScriptScanURLExclItemCount": OASRule(
        subcategory="ScriptScan (Windows only)",
        setting_name="URL exclusions count",
        translator=passthrough,
    ),
    "bApplyNVP": OASRule(
        subcategory="On-Access Scan",
        setting_name="Apply NVP",
        translator=enabled_disabled,
    ),
    "bScanArchives": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="Compressed archive files",
        translator=enabled_disabled,
    ),
    "bScanReading": OASRule(
        subcategory="Process Settings - When to Scan",
        setting_name="When reading from disk",
        translator=mapped({
            "0": "Do not scan when reading from or writing to disk",
            "1": "Let Trellix decide",
            "2": "Let me decide",
        }),
    ),
    "uAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Threat detection first response",
        translator=mapped({"1": "Clean", "2": "Delete"}),
    ),
    "uSecAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Threat detection if first response fail",
        translator=mapped({"1": "Clean", "2": "Delete"}),
    ),
    # extend with the rest of your cases
}


def _flatten_source_policy(source_policy: SourcePolicyRecord) -> list[tuple[str, str, str]]:
    if source_policy.payload_json is not None:
        return flatten_policy_json_sections(source_policy.payload_json)
    if source_policy.payload_xml:
        return flatten_policy_xml_sections(source_policy.payload_xml)
    return []


def build_oas_policy_setting_rows(source_policy: SourcePolicyRecord) -> list[PolicySettingRow]:
    parsed_name = parse_policy_name(source_policy.policy_name_full)
    flattened = _flatten_source_policy(source_policy)

    rows: list[PolicySettingRow] = []

    for section_name, setting_key, raw_value in flattened:
        rule = OAS_RULES.get(setting_key)
        if rule is None:
            continue

        rows.append(
            PolicySettingRow(
                source_policy_id=source_policy.source_policy_id,
                policy_type=source_policy.policy_type,
                policy_base_name=parsed_name.policy_base_name,
                policy_full_name=parsed_name.policy_full_name,
                version_label=parsed_name.version_label,
                section=section_name,
                subcategory=rule.subcategory,
                setting_key=setting_key,
                setting_name=rule.setting_name,
                raw_value=raw_value,
                display_value=rule.translator(raw_value),
            )
        )

    return rows


-- policy_rows/service.py

from __future__ import annotations

from .oas import build_oas_policy_setting_rows
from .ports import PolicySettingsRowRepository, SourcePolicyRepository


class PolicyRowsService:
    def __init__(
        self,
        source_repository: SourcePolicyRepository,
        row_repository: PolicySettingsRowRepository,
    ) -> None:
        self._source_repository = source_repository
        self._row_repository = row_repository

    def ingest_oas_policy_rows(
        self,
        *,
        policy_name_contains: str | None = None,
    ) -> dict[str, int]:
        source_policies = self._source_repository.list_policies(
            policy_type="OAS",
            policy_name_contains=policy_name_contains,
        )

        policies_read = 0
        rows_written = 0

        for source_policy in source_policies:
            rows = build_oas_policy_setting_rows(source_policy)

            self._row_repository.replace_rows_for_source_policy(
                source_policy_id=source_policy.source_policy_id,
                rows=rows,
            )

            policies_read += 1
            rows_written += len(rows)

        return {
            "policies_read": policies_read,
            "rows_written": rows_written,
        }


-- policy_rows/sql_repositories.py

from __future__ import annotations

import json
from typing import Any, Optional

from .models import PolicySettingRow, SourcePolicyRecord


class SqlSourcePolicyRepository:
    def __init__(self, get_session) -> None:
        self._get_session = get_session

    def list_policies(
        self,
        *,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        with self._get_session() as session:
            rows = self._retrieve_source_policy_rows(
                session,
                policy_type=policy_type,
                policy_name_contains=policy_name_contains,
            )

        return [self._map_source_row(row) for row in rows]

    def _retrieve_source_policy_rows(
        self,
        session,
        *,
        policy_type: Optional[str],
        policy_name_contains: Optional[str],
    ):
        # replace with your real existing query functions
        # example:
        # return trellix_queries.retrieve_policies(
        #     session,
        #     policy_type=policy_type,
        #     policy_name_contains=policy_name_contains,
        # )
        raise NotImplementedError

    @staticmethod
    def _map_source_row(row: Any) -> SourcePolicyRecord:
        payload_json = getattr(row, "payload_json", None)
        if isinstance(payload_json, str):
            payload_json = json.loads(payload_json)

        return SourcePolicyRecord(
            source_policy_id=str(getattr(row, "id")),
            policy_type=str(getattr(row, "policy_type")),
            policy_name_full=str(getattr(row, "policy_name")),
            payload_json=payload_json,
            payload_xml=getattr(row, "payload_xml", None),
        )


class SqlPolicySettingsRowRepository:
    def __init__(self, get_session) -> None:
        self._get_session = get_session

    def replace_rows_for_source_policy(
        self,
        *,
        source_policy_id: str,
        rows: list[PolicySettingRow],
    ) -> None:
        with self._get_session() as session:
            self._delete_existing_rows(session, source_policy_id=source_policy_id)
            self._insert_rows(session, rows=rows)
            session.commit()

    def _delete_existing_rows(self, session, *, source_policy_id: str) -> None:
        # replace with your real delete command
        # example:
        # trellix_commands.delete_oas_policy_rows(session, source_policy_id=source_policy_id)
        raise NotImplementedError

    def _insert_rows(self, session, *, rows: list[PolicySettingRow]) -> None:
        # replace with your real insert command
        # example:
        # trellix_commands.bulk_insert_oas_policy_rows(
        #     session,
        #     rows=[... mapped dicts / ORM models ...]
        # )
        raise NotImplementedError


-- policy_rows/mongo_repositories.py

from __future__ import annotations

from typing import Any, Optional

from .models import PolicySettingRow, SourcePolicyRecord


class MongoSourcePolicyRepository:
    def __init__(self, source_collection: Any) -> None:
        self._source_collection = source_collection

    def list_policies(
        self,
        *,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        query: dict[str, Any] = {}

        if policy_type:
            query["policy_type"] = policy_type

        if policy_name_contains:
            query["policy_name_full"] = {"$regex": policy_name_contains, "$options": "i"}

        docs = list(self._source_collection.find(query))

        return [
            SourcePolicyRecord(
                source_policy_id=str(doc["_id"]),
                policy_type=str(doc["policy_type"]),
                policy_name_full=str(doc["policy_name_full"]),
                payload_json=doc.get("payload_json"),
                payload_xml=doc.get("payload_xml"),
            )
            for doc in docs
        ]


class MongoPolicySettingsRowRepository:
    def __init__(self, target_collection: Any) -> None:
        self._target_collection = target_collection

    def replace_rows_for_source_policy(
        self,
        *,
        source_policy_id: str,
        rows: list[PolicySettingRow],
    ) -> None:
        self._target_collection.delete_many({"source_policy_id": source_policy_id})

        if not rows:
            return

        self._target_collection.insert_many(
            [
                {
                    "source_policy_id": row.source_policy_id,
                    "policy_type": row.policy_type,
                    "policy_base_name": row.policy_base_name,
                    "policy_full_name": row.policy_full_name,
                    "version_label": row.version_label,
                    "section": row.section,
                    "subcategory": row.subcategory,
                    "setting_key": row.setting_key,
                    "setting_name": row.setting_name,
                    "raw_value": row.raw_value,
                    "display_value": row.display_value,
                }
                for row in rows
            ]
        )


-- policy_rows/bootstrap.py

from __future__ import annotations

from .mongo_repositories import (
    MongoPolicySettingsRowRepository,
    MongoSourcePolicyRepository,
)
from .service import PolicyRowsService
from .sql_repositories import (
    SqlPolicySettingsRowRepository,
    SqlSourcePolicyRepository,
)


def build_sql_policy_rows_service(get_session) -> PolicyRowsService:
    source_repository = SqlSourcePolicyRepository(get_session)
    row_repository = SqlPolicySettingsRowRepository(get_session)
    return PolicyRowsService(source_repository, row_repository)


def build_mongo_policy_rows_service(
    *,
    source_collection,
    target_collection,
) -> PolicyRowsService:
    source_repository = MongoSourcePolicyRepository(source_collection)
    row_repository = MongoPolicySettingsRowRepository(target_collection)
    return PolicyRowsService(source_repository, row_repository)


-- usage 

service = build_sql_policy_rows_service(get_session)
result = service.ingest_oas_policy_rows()

service = build_mongo_policy_rows_service(
    source_collection=...,
    target_collection=...,
)
result = service.ingest_oas_policy_rows()

'''
Notes:
The clean way is not to make queries.py / commands.py accept "sql" or "mongo" flags.

Use ports and adapters:

functional core: pure OAS transform logic
application layer: orchestration, depends on interfaces
infrastructure adapters: SQL implementation, Mongo implementation
composition root: picks which adapter to use

So the switch happens in wiring, not inside business logic.




'''

'''
tests layout
server_code/services/trellix/policy_rows/
    common.py
    models.py
    oas.py
    ports.py
    service.py

tests/
    trellix/
        policy_rows/
            test_common.py
            test_oas.py
            test_service.py
'''

-- tests/trellix/policy_rows/test_common.py

from server_code.services.trellix.policy_rows.common import (
    flatten_policy_json_sections,
    flatten_policy_xml_sections,
    parse_policy_name,
    stringify,
)


def test_parse_policy_name_with_version_suffix():
    result = parse_policy_name("GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings")

    assert result.policy_base_name == "GLOBAL_STD_SRV_ENS_TP_OAS (copy)"
    assert result.policy_full_name == "GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings"
    assert result.version_label == "Settings"


def test_parse_policy_name_without_version_suffix():
    result = parse_policy_name("GLOBAL_STD_SRV_ENS_TP_OAS")

    assert result.policy_base_name == "GLOBAL_STD_SRV_ENS_TP_OAS"
    assert result.policy_full_name == "GLOBAL_STD_SRV_ENS_TP_OAS"
    assert result.version_label is None


def test_stringify_handles_none():
    assert stringify(None) == ""


def test_stringify_handles_other_values():
    assert stringify(1) == "1"
    assert stringify(True) == "True"
    assert stringify("abc") == "abc"


def test_flatten_policy_json_sections():
    payload_json = {
        "policy_name": "GLOBAL_STD_SRV_ENS_TP_OAS",
        "sections": {
            "Alerting": {
                "bShowAlerts": "1",
                "szDialogMessage": "Malware detected",
            },
            "General": {
                "bOASEnabled": "1",
            },
        },
    }

    result = flatten_policy_json_sections(payload_json)

    assert result == [
        ("Alerting", "bShowAlerts", "1"),
        ("Alerting", "szDialogMessage", "Malware detected"),
        ("General", "bOASEnabled", "1"),
    ]


def test_flatten_policy_json_sections_ignores_non_dict_sections():
    payload_json = {
        "sections": {
            "Alerting": {
                "bShowAlerts": "1",
            },
            "BrokenSection": ["a", "b", "c"],
        },
    }

    result = flatten_policy_json_sections(payload_json)

    assert result == [
        ("Alerting", "bShowAlerts", "1"),
    ]


def test_flatten_policy_xml_sections():
    payload_xml = """
    <EPOPolicySettings name="GLOBAL_STD_SRV_ENS_TP_OAS">
        <Section name="Alerting">
            <Setting name="bShowAlerts" value="1"/>
            <Setting name="szDialogMessage" value="Malware detected"/>
        </Section>
        <Section name="General">
            <Setting name="bOASEnabled" value="1"/>
        </Section>
    </EPOPolicySettings>
    """

    result = flatten_policy_xml_sections(payload_xml)

    assert result == [
        ("Alerting", "bShowAlerts", "1"),
        ("Alerting", "szDialogMessage", "Malware detected"),
        ("General", "bOASEnabled", "1"),
    ]


-- tests/trellix/policy_rows/test_oas.py

import pytest

from server_code.services.trellix.policy_rows.models import SourcePolicyRecord
from server_code.services.trellix.policy_rows.oas import (
    build_oas_policy_setting_rows,
    enabled_disabled,
    flatten_source_policy,
    mapped,
)


def test_enabled_disabled_translation():
    assert enabled_disabled("1") == "Enabled"
    assert enabled_disabled("0") == "Disabled"
    assert enabled_disabled("abc") == "abc"


def test_mapped_translation():
    translator = mapped({"1": "Clean", "2": "Delete"})

    assert translator("1") == "Clean"
    assert translator("2") == "Delete"
    assert translator("999") == "999"


def test_flatten_source_policy_prefers_json_when_present():
    source_policy = SourcePolicyRecord(
        source_policy_id="1",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS::Settings",
        payload_json={
            "sections": {
                "General": {
                    "bOASEnabled": "1",
                }
            }
        },
        payload_xml="""
        <EPOPolicySettings name="GLOBAL_STD_SRV_ENS_TP_OAS::Settings">
            <Section name="General">
                <Setting name="bOASEnabled" value="0"/>
            </Section>
        </EPOPolicySettings>
        """,
    )

    result = flatten_source_policy(source_policy)

    assert result == [
        ("General", "bOASEnabled", "1"),
    ]


def test_build_oas_policy_setting_rows_from_json():
    source_policy = SourcePolicyRecord(
        source_policy_id="123",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings",
        payload_json={
            "sections": {
                "Alerting": {
                    "bShowAlerts": "1",
                    "szDialogMessage": "Malware detected and handled",
                },
                "General": {
                    "scanUsingAMSIHooks": "0",
                },
                "GTI": {
                    "GTISensitivityLevel": "1",
                },
            }
        },
        payload_xml=None,
    )

    result = build_oas_policy_setting_rows(source_policy)

    assert len(result) == 4

    assert result[0].source_policy_id == "123"
    assert result[0].policy_type == "OAS"
    assert result[0].policy_base_name == "GLOBAL_STD_SRV_ENS_TP_OAS (copy)"
    assert result[0].policy_full_name == "GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings"
    assert result[0].version_label == "Settings"

    assert result[0].section == "Alerting"
    assert result[0].setting_key == "bShowAlerts"
    assert result[0].setting_name == "Display the On-Access Scan window to users when a threat is detected"
    assert result[0].display_value == "Enabled"

    assert result[1].setting_key == "szDialogMessage"
    assert result[1].display_value == "Malware detected and handled"

    assert result[2].setting_key == "scanUsingAMSIHooks"
    assert result[2].display_value == "Disabled"

    assert result[3].setting_key == "GTISensitivityLevel"
    assert result[3].display_value == "Enabled"


def test_build_oas_policy_setting_rows_from_xml():
    source_policy = SourcePolicyRecord(
        source_policy_id="456",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS",
        payload_json=None,
        payload_xml="""
        <EPOPolicySettings name="GLOBAL_STD_SRV_ENS_TP_OAS">
            <Section name="ScriptScan">
                <Setting name="scriptScanEnabled" value="0"/>
            </Section>
            <Section name="ScriptScanURLExclItems">
                <Setting name="dwScriptScanURLExclItemCount" value="7"/>
            </Section>
        </EPOPolicySettings>
        """,
    )

    result = build_oas_policy_setting_rows(source_policy)

    assert len(result) == 2

    assert result[0].policy_base_name == "GLOBAL_STD_SRV_ENS_TP_OAS"
    assert result[0].version_label is None
    assert result[0].setting_key == "scriptScanEnabled"
    assert result[0].display_value == "Disabled"

    assert result[1].setting_key == "dwScriptScanURLExclItemCount"
    assert result[1].display_value == "7"


def test_build_oas_policy_setting_rows_ignores_unknown_keys():
    source_policy = SourcePolicyRecord(
        source_policy_id="789",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS",
        payload_json={
            "sections": {
                "General": {
                    "unknownSettingKey": "abc",
                    "bShowAlerts": "1",
                }
            }
        },
        payload_xml=None,
    )

    result = build_oas_policy_setting_rows(source_policy)

    assert len(result) == 1
    assert result[0].setting_key == "bShowAlerts"


def test_build_oas_policy_setting_rows_empty_payload():
    source_policy = SourcePolicyRecord(
        source_policy_id="999",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS",
        payload_json=None,
        payload_xml=None,
    )

    result = build_oas_policy_setting_rows(source_policy)

    assert result == []


def test_build_oas_policy_setting_rows_with_monkeypatched_flatten(monkeypatch):
    source_policy = SourcePolicyRecord(
        source_policy_id="111",
        policy_type="OAS",
        policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS::Settings",
        payload_json=None,
        payload_xml=None,
    )

    monkeypatch.setattr(
        "server_code.services.trellix.policy_rows.oas.flatten_source_policy",
        lambda _: [
            ("General", "bShowAlerts", "0"),
            ("General", "scanUsingAMSIHooks", "1"),
        ],
    )

    result = build_oas_policy_setting_rows(source_policy)

    assert len(result) == 2
    assert [row.display_value for row in result] == ["Disabled", "Enabled"]


-- tests/trellix/policy_rows/test_service.py

from dataclasses import dataclass

from server_code.services.trellix.policy_rows.models import (
    PolicySettingRow,
    SourcePolicyRecord,
)
from server_code.services.trellix.policy_rows.service import PolicyRowsService


@dataclass
class InMemorySourcePolicyRepository:
    policies: list[SourcePolicyRecord]

    def list_policies(
        self,
        *,
        policy_type=None,
        policy_name_contains=None,
    ) -> list[SourcePolicyRecord]:
        result = self.policies

        if policy_type is not None:
            result = [p for p in result if p.policy_type == policy_type]

        if policy_name_contains is not None:
            needle = policy_name_contains.lower()
            result = [p for p in result if needle in p.policy_name_full.lower()]

        return result


class InMemoryPolicySettingsRowRepository:
    def __init__(self) -> None:
        self.storage: dict[str, list[PolicySettingRow]] = {}

    def replace_rows_for_source_policy(
        self,
        *,
        source_policy_id: str,
        rows: list[PolicySettingRow],
    ) -> None:
        self.storage[source_policy_id] = list(rows)


def test_ingest_oas_policy_rows_only_processes_oas():
    source_repo = InMemorySourcePolicyRepository(
        policies=[
            SourcePolicyRecord(
                source_policy_id="1",
                policy_type="OAS",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS::Settings",
                payload_json={
                    "sections": {
                        "Alerting": {
                            "bShowAlerts": "1",
                        }
                    }
                },
                payload_xml=None,
            ),
            SourcePolicyRecord(
                source_policy_id="2",
                policy_type="FW",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_FW::Settings",
                payload_json={
                    "sections": {
                        "General": {
                            "something": "1",
                        }
                    }
                },
                payload_xml=None,
            ),
        ]
    )
    row_repo = InMemoryPolicySettingsRowRepository()

    service = PolicyRowsService(source_repo, row_repo)
    result = service.ingest_oas_policy_rows()

    assert result == {
        "policies_read": 1,
        "rows_written": 1,
    }

    assert "1" in row_repo.storage
    assert "2" not in row_repo.storage

    stored_rows = row_repo.storage["1"]
    assert len(stored_rows) == 1
    assert stored_rows[0].setting_key == "bShowAlerts"
    assert stored_rows[0].display_value == "Enabled"


def test_ingest_oas_policy_rows_replaces_existing_rows():
    source_repo = InMemorySourcePolicyRepository(
        policies=[
            SourcePolicyRecord(
                source_policy_id="1",
                policy_type="OAS",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS::Settings",
                payload_json={
                    "sections": {
                        "General": {
                            "scriptScanEnabled": "0",
                        }
                    }
                },
                payload_xml=None,
            )
        ]
    )
    row_repo = InMemoryPolicySettingsRowRepository()
    row_repo.storage["1"] = [
        PolicySettingRow(
            source_policy_id="1",
            policy_type="OAS",
            policy_base_name="OLD",
            policy_full_name="OLD",
            version_label=None,
            section="Old",
            subcategory="Old",
            setting_key="old",
            setting_name="old",
            raw_value="old",
            display_value="old",
        )
    ]

    service = PolicyRowsService(source_repo, row_repo)
    result = service.ingest_oas_policy_rows()

    assert result == {
        "policies_read": 1,
        "rows_written": 1,
    }

    stored_rows = row_repo.storage["1"]
    assert len(stored_rows) == 1
    assert stored_rows[0].setting_key == "scriptScanEnabled"
    assert stored_rows[0].display_value == "Disabled"


def test_ingest_oas_policy_rows_filters_by_name_contains():
    source_repo = InMemorySourcePolicyRepository(
        policies=[
            SourcePolicyRecord(
                source_policy_id="1",
                policy_type="OAS",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS Control-M::Settings",
                payload_json={
                    "sections": {
                        "Alerting": {
                            "bShowAlerts": "1",
                        }
                    }
                },
                payload_xml=None,
            ),
            SourcePolicyRecord(
                source_policy_id="2",
                policy_type="OAS",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS Citrix::Settings",
                payload_json={
                    "sections": {
                        "Alerting": {
                            "bShowAlerts": "0",
                        }
                    }
                },
                payload_xml=None,
            ),
        ]
    )
    row_repo = InMemoryPolicySettingsRowRepository()

    service = PolicyRowsService(source_repo, row_repo)
    result = service.ingest_oas_policy_rows(policy_name_contains="Control-M")

    assert result == {
        "policies_read": 1,
        "rows_written": 1,
    }

    assert "1" in row_repo.storage
    assert "2" not in row_repo.storage
    assert row_repo.storage["1"][0].display_value == "Enabled"


