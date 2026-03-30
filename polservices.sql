from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol
from xml.etree import ElementTree as ET
import json


# ============================================================
# MODELS
# ============================================================

@dataclass(frozen=True)
class SourcePolicyRecord:
    source_id: str
    policy_type: str
    policy_name_full: str
    payload_json: Optional[dict[str, Any]] = None
    payload_xml: Optional[str] = None
    source_updated_at: Optional[str] = None
    source_system: str = "sql"


@dataclass(frozen=True)
class ParsedPolicyName:
    policy_base_name: str
    policy_full_name: str
    version_label: Optional[str]


@dataclass(frozen=True)
class OASPolicySettingRow:
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

    sort_key: str


@dataclass(frozen=True)
class IngestResult:
    policies_read: int
    rows_written: int


# ============================================================
# PROTOCOLS / DI INTERFACES
# ============================================================

class PolicySourceRepository(Protocol):
    def fetch_policies(
        self,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        ...


class PolicyRowSink(Protocol):
    def save_oas_policy_settings_rows(self, rows: list[OASPolicySettingRow]) -> None:
        ...


# ============================================================
# HELPERS
# ============================================================

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


def to_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def enabled_disabled(value: str) -> str:
    if value == "1":
        return "Enabled"
    if value == "0":
        return "Disabled"
    return value


def passthrough(value: str) -> str:
    return value


def map_from_dict(mapping: dict[str, str]):
    def _translate(value: str) -> str:
        return mapping.get(value, value)
    return _translate


# ============================================================
# OAS RULES
# Only policy settings rows for now.
# Applications and exclusions intentionally left for later.
# ============================================================

@dataclass(frozen=True)
class OASRule:
    subcategory: str
    setting_name: str
    translator: Any


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
    "bAllowDisableViaMcTray": OASRule(
        subcategory="On-Access Scan",
        setting_name="Allow users to disable On-Access Scan from the Trellix system tray",
        translator=enabled_disabled,
    ),
    "bEnforceMaxScanTime": OASRule(
        subcategory="On-Access Scan",
        setting_name="Specify maximum number of seconds for each file scan",
        translator=enabled_disabled,
    ),
    "bOASEnabled": OASRule(
        subcategory="On-Access Scan",
        setting_name="Enable On-Access Scan",
        translator=enabled_disabled,
    ),
    "bOnlyUseDefaultConfig": OASRule(
        subcategory="Process Settings",
        setting_name="Use Standard settings for all processes",
        translator=enabled_disabled,
    ),
    "bScanBootSectors": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="Boot Sectors",
        translator=enabled_disabled,
    ),
    "bStartEnabled": OASRule(
        subcategory="On-Access Scan",
        setting_name="Enable On-Access Scan on system startup (Windows only)",
        translator=enabled_disabled,
    ),
    "dwScannerThreadTimeout": OASRule(
        subcategory="On-Access Scan",
        setting_name="Maximum number of seconds for each file scan",
        translator=passthrough,
    ),
    "enableAMSIObserveMode": OASRule(
        subcategory="Antimalware Scan Interface (Windows only)",
        setting_name="Enable Observe mode (Events are generated but actions are not enforced)",
        translator=enabled_disabled,
    ),
    "scanCopyLocalFolders": OASRule(
        subcategory="On-Access Scan",
        setting_name="Scan when copying between local folders (Windows only)",
        translator=enabled_disabled,
    ),
    "scanCopyNetworkRemovable": OASRule(
        subcategory="On-Access Scan",
        setting_name="Scan when copying from network folders and removable drives",
        translator=enabled_disabled,
    ),
    "scanEmailAttachments": OASRule(
        subcategory="On-Access Scan",
        setting_name="Detect suspicious email attachments (Windows only)",
        translator=enabled_disabled,
    ),
    "scanProcessesOnEnable": OASRule(
        subcategory="On-Access Scan",
        setting_name="Scan processes on service startup and content update (Windows only)",
        translator=enabled_disabled,
    ),
    "scanShadowCopyDisableStatus": OASRule(
        subcategory="On-Access Scan",
        setting_name="Disable read/write scan of Shadow Copy volumes for SYSTEM process",
        translator=enabled_disabled,
    ),
    "scanTrustedInstallers": OASRule(
        subcategory="On-Access Scan",
        setting_name="Scan trusted installers (Windows only)",
        translator=enabled_disabled,
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
    "bNetworkScanEnabled": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="On network drives",
        translator=enabled_disabled,
    ),
    "bScanArchives": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="Compressed archive files",
        translator=enabled_disabled,
    ),
    "bScanBackupReads": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="Opened for backups (Windows only)",
        translator=enabled_disabled,
    ),
    "bScanMime": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="Compressed MIME - encoded files",
        translator=enabled_disabled,
    ),
    "bScanReading": OASRule(
        subcategory="Process Settings - When to Scan",
        setting_name="When reading from disk",
        translator=map_from_dict({
            "0": "Do not scan when reading from or writing to disk",
            "1": "Let Trellix decide",
            "2": "Let me decide",
        }),
    ),
    "bScanReadingByPass": OASRule(
        subcategory="Process Settings - When to Scan",
        setting_name="When reading from disk bypass",
        translator=map_from_dict({
            "0": "Do not scan when reading from or writing to disk",
            "1": "Let Trellix decide",
            "2": "Let me decide",
        }),
    ),
    "bScanWriting": OASRule(
        subcategory="Process Settings - When to Scan",
        setting_name="When writing to disk",
        translator=map_from_dict({
            "0": "Do not scan when reading from or writing to disk",
            "1": "Let Trellix decide",
            "2": "Let me decide",
        }),
    ),
    "bScanWritingByPass": OASRule(
        subcategory="Process Settings - When to Scan",
        setting_name="When writing to disk bypass",
        translator=map_from_dict({
            "0": "Do not scan when reading from or writing to disk",
            "1": "Let Trellix decide",
            "2": "Let me decide",
        }),
    ),
    "extensionMode": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="What to Scan",
        translator=map_from_dict({
            "1": "All files",
            "2": "Default and specified file types",
            "3": "Specified file types only",
        }),
    ),
    "szProgExts": OASRule(
        subcategory="Process Settings - What to Scan",
        setting_name="File extensions",
        translator=passthrough,
    ),
    "uAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Threat detection first response",
        translator=map_from_dict({
            "1": "Clean",
            "2": "Delete",
        }),
    ),
    "uAction_Program": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Unwanted program first response",
        translator=map_from_dict({
            "1": "Clean",
            "2": "Delete",
        }),
    ),
    "uSecAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Threat detection if first response fail",
        translator=map_from_dict({
            "1": "Clean",
            "2": "Delete",
        }),
    ),
    "uSecAction_Program": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="Unwanted program if first response fail",
        translator=map_from_dict({
            "1": "Clean",
            "2": "Delete",
        }),
    ),
    "uScanErrorAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="On Timeout: (Linux Only)",
        translator=map_from_dict({
            "3": "Allow access to files",
            "4": "Deny access to files",
        }),
    ),
    "uTimeOutAction": OASRule(
        subcategory="Process Settings - Actions",
        setting_name="On Scan Error: (Linux Only)",
        translator=map_from_dict({
            "3": "Allow access to files",
            "4": "Deny access to files",
        }),
    ),
    "bUnknownMacroHeuristics": OASRule(
        subcategory="Additional Scan Options",
        setting_name="Detect Unknown macro threats",
        translator=enabled_disabled,
    ),
    "bUnknownProgramHeuristics": OASRule(
        subcategory="Additional Scan Options",
        setting_name="Detect Unknown program threats",
        translator=enabled_disabled,
    ),
    "systemUtilization": OASRule(
        subcategory="Performance",
        setting_name="System Utilization",
        translator=map_from_dict({
            "0": "Disabled",
            "1": "Low",
            "2": "Below Normal",
            "3": "Normal",
        }),
    ),
}


# ============================================================
# PARSERS
# ============================================================

def flatten_oas_json_policy_sections(payload_json: dict[str, Any]) -> list[tuple[str, str, str]]:
    """
    Returns list of tuples:
    (section_name, setting_key, raw_value)
    """
    sections = payload_json.get("sections", {})
    flattened: list[tuple[str, str, str]] = []

    for section_name, section_settings in sections.items():
        if not isinstance(section_settings, dict):
            continue

        for setting_key, raw_value in section_settings.items():
            flattened.append((section_name, setting_key, to_string(raw_value)))

    return flattened


def flatten_oas_xml_policy_sections(payload_xml: str) -> list[tuple[str, str, str]]:
    root = ET.fromstring(payload_xml)
    flattened: list[tuple[str, str, str]] = []

    for section_el in root.findall("./Section"):
        section_name = section_el.attrib.get("name", "").strip()
        for setting_el in section_el.findall("./Setting"):
            setting_key = setting_el.attrib.get("name", "").strip()
            raw_value = setting_el.attrib.get("value", "")
            flattened.append((section_name, setting_key, raw_value))

    return flattened


def get_flat_settings(source_policy: SourcePolicyRecord) -> list[tuple[str, str, str]]:
    if source_policy.payload_json is not None:
        return flatten_oas_json_policy_sections(source_policy.payload_json)

    if source_policy.payload_xml:
        return flatten_oas_xml_policy_sections(source_policy.payload_xml)

    return []


# ============================================================
# TRANSFORMER
# ============================================================

class OASTransformerError(Exception):
    pass


class OASTransformer:
    def transform_policy(self, source_policy: SourcePolicyRecord) -> list[OASPolicySettingRow]:
        if source_policy.policy_type.strip().upper() != "OAS":
            raise OASTransformerError(
                f"Policy {source_policy.source_id} is not OAS. Got: {source_policy.policy_type}"
            )

        parsed_name = parse_policy_name(source_policy.policy_name_full)
        flat_settings = get_flat_settings(source_policy)

        rows: list[OASPolicySettingRow] = []

        for section_name, setting_key, raw_value in flat_settings:
            rule = OAS_RULES.get(setting_key)
            if rule is None:
                # For settings view only, ignore unsupported keys like Application/Exclusions for now
                continue

            display_value = rule.translator(raw_value)
            sort_key = f"{section_name}|{rule.subcategory}|{rule.setting_name}"

            rows.append(
                OASPolicySettingRow(
                    source_policy_id=source_policy.source_id,
                    policy_type=source_policy.policy_type,
                    policy_base_name=parsed_name.policy_base_name,
                    policy_full_name=parsed_name.policy_full_name,
                    version_label=parsed_name.version_label,
                    section=section_name,
                    subcategory=rule.subcategory,
                    setting_key=setting_key,
                    setting_name=rule.setting_name,
                    raw_value=raw_value,
                    display_value=display_value,
                    sort_key=sort_key,
                )
            )

        return rows


# ============================================================
# REPOSITORY EXAMPLES
# ============================================================

class MemoryPolicyRowSink:
    def __init__(self) -> None:
        self.saved_rows: list[OASPolicySettingRow] = []

    def save_oas_policy_settings_rows(self, rows: list[OASPolicySettingRow]) -> None:
        self.saved_rows.extend(rows)


class PrintPolicyRowSink:
    def save_oas_policy_settings_rows(self, rows: list[OASPolicySettingRow]) -> None:
        for row in rows:
            print({
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
                "sort_key": row.sort_key,
            })


class MongoPolicyRowSink:
    """
    Placeholder synchronous sink.
    Replace `insert_many` with your real Mongo client call when ready.
    """

    def __init__(self, collection: Any) -> None:
        self._collection = collection

    def save_oas_policy_settings_rows(self, rows: list[OASPolicySettingRow]) -> None:
        if not rows:
            return

        documents = [
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
                "sort_key": row.sort_key,
            }
            for row in rows
        ]

        self._collection.insert_many(documents)


class InMemoryPolicySourceRepository:
    """
    Test/demonstration repository.
    Useful until your real SQL query class is wired in.
    """

    def __init__(self, policies: list[SourcePolicyRecord]) -> None:
        self._policies = policies

    def fetch_policies(
        self,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        results = self._policies

        if policy_type:
            policy_type_upper = policy_type.upper()
            results = [
                p for p in results
                if p.policy_type.upper() == policy_type_upper
            ]

        if policy_name_contains:
            needle = policy_name_contains.lower()
            results = [
                p for p in results
                if needle in p.policy_name_full.lower()
            ]

        return results


# ============================================================
# SQL ADAPTER EXAMPLE
# Replace the query body with your real SQL method.
# ============================================================

class SqlPolicySourceRepository:
    """
    Adapter around your existing SQL access method.

    You said you already have a ready method that returns JSON or XML from SQL.
    Plug that method into `fetch_raw_policies_from_sql`.
    """

    def __init__(self, sql_gateway: Any) -> None:
        self._sql_gateway = sql_gateway

    def fetch_policies(
        self,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[SourcePolicyRecord]:
        raw_rows = self.fetch_raw_policies_from_sql(
            policy_type=policy_type,
            policy_name_contains=policy_name_contains,
        )

        records: list[SourcePolicyRecord] = []

        for row in raw_rows:
            payload_json = row.get("payload_json")
            payload_xml = row.get("payload_xml")

            if isinstance(payload_json, str):
                payload_json = json.loads(payload_json)

            records.append(
                SourcePolicyRecord(
                    source_id=to_string(row["source_id"]),
                    policy_type=to_string(row["policy_type"]),
                    policy_name_full=to_string(row["policy_name_full"]),
                    payload_json=payload_json,
                    payload_xml=payload_xml,
                    source_updated_at=row.get("source_updated_at"),
                    source_system="sql",
                )
            )

        return records

    def fetch_raw_policies_from_sql(
        self,
        policy_type: Optional[str] = None,
        policy_name_contains: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Replace this method with your real SQL access.

        Expected output shape per row:
        {
            "source_id": "123",
            "policy_type": "OAS",
            "policy_name_full": "GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings (...)",
            "payload_json": {...} or JSON string,
            "payload_xml": None or "<EPOPolicySettings ...>",
            "source_updated_at": "2026-03-27T10:00:00"
        }
        """
        return self._sql_gateway.get_policies(
            policy_type=policy_type,
            policy_name_contains=policy_name_contains,
        )


# ============================================================
# QUERY / INGEST SERVICE
# ============================================================

class PolicyIngestService:
    def __init__(
        self,
        source_repository: PolicySourceRepository,
        row_sink: PolicyRowSink,
        oas_transformer: Optional[OASTransformer] = None,
    ) -> None:
        self._source_repository = source_repository
        self._row_sink = row_sink
        self._oas_transformer = oas_transformer or OASTransformer()

    def ingest_oas_policies(
        self,
        policy_name_contains: Optional[str] = None,
    ) -> IngestResult:
        source_policies = self._source_repository.fetch_policies(
            policy_type="OAS",
            policy_name_contains=policy_name_contains,
        )

        all_rows: list[OASPolicySettingRow] = []

        for source_policy in source_policies:
            rows = self._oas_transformer.transform_policy(source_policy)
            all_rows.extend(rows)

        self._row_sink.save_oas_policy_settings_rows(all_rows)

        return IngestResult(
            policies_read=len(source_policies),
            rows_written=len(all_rows),
        )


# ============================================================
# OPTIONAL: helper for future comparison / baseline work
# ============================================================

def build_keyed_settings_map(rows: list[OASPolicySettingRow]) -> dict[tuple[str, str, str], str]:
    """
    Useful later for baseline comparison.

    Key:
      (section, subcategory, setting_name)
    Value:
      display_value
    """
    result: dict[tuple[str, str, str], str] = {}

    for row in rows:
        key = (row.section, row.subcategory, row.setting_name)
        result[key] = row.display_value or ""

    return result


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    sample_policy_json = {
        "policy_name": "GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings",
        "sections": {
            "Alerting": {
                "bShowAlerts": "1",
                "szDialogMessage": "Malware detected and handled by Trellix Endpoint Security",
            },
            "General": {
                "bAllowDisableViaMcTray": "0",
                "bEnforceMaxScanTime": "1",
                "bOASEnabled": "1",
                "bOnlyUseDefaultConfig": "0",
                "bScanBootSectors": "1",
                "bStartEnabled": "1",
                "dwScannerThreadTimeout": "45",
                "enableAMSIObserveMode": "0",
                "scanCopyLocalFolders": "0",
                "scanCopyNetworkRemovable": "1",
                "scanEmailAttachments": "0",
                "scanProcessesOnEnable": "0",
                "scanShadowCopyDisableStatus": "0",
                "scanTrustedInstallers": "0",
                "scanUsingAMSIHooks": "1",
            },
            "GTI": {
                "GTISensitivityLevel": "0"
            },
            "ScriptScan": {
                "scriptScanEnabled": "0"
            },
            "ScriptScanURLExclItems": {
                "dwScriptScanURLExclItemCount": "0"
            },
            "Default-Detection": {
                "bApplyNVP": "1",
                "bNetworkScanEnabled": "0",
                "bScanArchives": "0",
                "bScanBackupReads": "1",
                "bScanMime": "1",
                "bScanReading": "1",
                "bScanReadingByPass": "1",
                "bScanWriting": "1",
                "bScanWritingByPass": "1",
                "bUnknownMacroHeuristics": "1",
                "bUnknownProgramHeuristics": "1",
                "extensionMode": "2",
                "szProgExts": "",
                "uAction": "1",
                "uAction_Program": "1",
                "uScanErrorAction": "3",
                "uSecAction": "2",
                "uSecAction_Program": "2",
                "uTimeOutAction": "4",
            },
        },
    }

    repo = InMemoryPolicySourceRepository(
        policies=[
            SourcePolicyRecord(
                source_id="1",
                policy_type="OAS",
                policy_name_full="GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings",
                payload_json=sample_policy_json,
            )
        ]
    )

    sink = PrintPolicyRowSink()
    service = PolicyIngestService(source_repository=repo, row_sink=sink)

    result = service.ingest_oas_policies()
    print("RESULT:", result)


example = {
  "source_policy_id": "1",
  "policy_type": "OAS",
  "policy_base_name": "GLOBAL_STD_SRV_ENS_TP_OAS (copy)",
  "policy_full_name": "GLOBAL_STD_SRV_ENS_TP_OAS (copy)::Settings",
  "version_label": "Settings",
  "section": "General",
  "subcategory": "Antimalware Scan Interface (Windows only)",
  "setting_key": "scanUsingAMSIHooks",
  "setting_name": "Enable AMSI (provides enhanced script scanning) (Windows only)",
  "raw_value": "1",
  "display_value": "Enabled",
  "sort_key": "General|Antimalware Scan Interface (Windows only)|Enable AMSI (provides enhanced script scanning) (Windows only)"
}