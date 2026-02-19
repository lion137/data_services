"""
lxml_tips_and_patterns.py

A single, self-contained "cheat sheet" for working with large, namespaced XML exports
like your Trelix/ePO policy exports.

What this file includes:
- How to load and inspect namespaces
- Common XPath patterns (find settings, objects, sections, settings)
- Pretty printing and debugging helpers
- Building a canonical representation of policy settings
- Normalization (whitespace + list-like settings such as ExcludedItem_0..N)
- Semantic diff (baseline vs incoming) on the canonical representation

Run:
    python lxml_tips_and_patterns.py baseline.xml incoming.xml

Note:
- This is intentionally verbose with comments (python-style) to serve as reference.
- Adjust LISTY_PREFIXES / COUNT_NAMES to your exact policy export patterns.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from lxml import etree


# ------------------------------------------------------------------------------
# 1) LOADING + NAMESPACE HANDLING
# ------------------------------------------------------------------------------

def parse_xml(path: str | Path) -> tuple[etree._ElementTree, etree._Element]:
    """
    Parse an XML file from disk.

    lxml returns:
      - ElementTree (tree)
      - root element (root)

    Use huge_tree=True for very large files; it relaxes some limits and can help
    with "entity expansion"/depth constraints in huge docs.
    """
    parser = etree.XMLParser(
        remove_blank_text=False,  # keep as-is; we do our own normalization later
        huge_tree=True,
        recover=False,            # set True only if the XML might be malformed
    )
    tree = etree.parse(str(path), parser)
    root = tree.getroot()
    return tree, root


def build_nsmap(root: etree._Element) -> dict[str, str]:
    """
    Build a namespace dict for XPath queries.

    Important lxml rule:
    - If the XML uses a default namespace (xmlns="..."), you STILL must bind it
      to a prefix in your XPath (e.g. 'epo'), then use that prefix.

    Your screenshots suggest something like:
      <epo:EPOPolicySchema xmlns:epo="mcafee-epo-policy" ...>
    but sometimes it can be default namespace; this handles both.

    Returns a dict like {"epo": "mcafee-epo-policy"} or {} if no namespace.
    """
    # Prefer 'epo' prefix if present, else fall back to default namespace (None).
    uri = root.nsmap.get("epo") or root.nsmap.get(None)
    return {"epo": uri} if uri else {}


def debug_ns(root: etree._Element) -> None:
    """
    Print the namespace mappings visible in the root element.
    """
    print("root tag:", root.tag)
    print("nsmap:", root.nsmap)


# ------------------------------------------------------------------------------
# 2) XPATH BASICS / ELEMENT FINDING
# ------------------------------------------------------------------------------

def xpath(root: etree._Element, expr: str, NS: dict[str, str]) -> list[Any]:
    """
    Thin wrapper so you can type less.
    """
    return root.xpath(expr, namespaces=NS)


def find_all_policy_settings(root: etree._Element, NS: dict[str, str]) -> list[etree._Element]:
    """
    Find all <EPOPolicySettings ...> blocks.
    """
    return xpath(root, ".//epo:EPOPolicySettings", NS)


def find_all_policy_objects(root: etree._Element, NS: dict[str, str]) -> list[etree._Element]:
    """
    Find all <EPOPolicyObject ...> blocks.
    """
    return xpath(root, ".//epo:EPOPolicyObject", NS)


def find_setting_nodes_by_name(root: etree._Element, setting_name: str, NS: dict[str, str]) -> list[etree._Element]:
    """
    Find all <Setting name="..."> anywhere in the document.

    Tip:
    - This is great for exploration:
        find_setting_nodes_by_name(root, "dwExclusionCount", NS)
    """
    return xpath(root, f".//epo:Setting[@name='{setting_name}']", NS)


# ------------------------------------------------------------------------------
# 3) PRINTING / DEBUGGING ELEMENTS
# ------------------------------------------------------------------------------

def element_preview(elem: etree._Element, max_chars: int = 2000) -> str:
    """
    Pretty print a single element to a string for debugging.

    Tip:
    - In big XML files, print only first N chars to avoid flooding console.
    """
    s = etree.tostring(elem, pretty_print=True, encoding="unicode")
    return s[:max_chars] + ("..." if len(s) > max_chars else "")


def element_path(elem: etree._Element) -> str:
    """
    Get a readable XPath-ish path for an element.
    """
    return elem.getroottree().getpath(elem)


# ------------------------------------------------------------------------------
# 4) CANONICAL EXTRACTION: EPOPolicySettings -> {sections -> {setting -> value}}
# ------------------------------------------------------------------------------

def parse_policy_settings_block(ps: etree._Element, NS: dict[str, str]) -> dict[str, Any]:
    """
    Convert one <EPOPolicySettings> block into a canonical dict:

    {
      "meta": { "name": ..., "featureid": ..., "categoryid": ..., "typeid": ... },
      "sections": {
         "Default-Detection": { "bApplyNVP": "1", ... },
         "Default-Detection_Exclusions": { "ExcludedItem_0": "3|3|C:\\...", ... },
      }
    }

    Tip:
    - This is the key step for semantic diffs. Raw XML diffs are too noisy.
    """
    meta = {k: (ps.get(k) or "") for k in ("name", "featureid", "categoryid", "typeid")}

    sections: dict[str, dict[str, str]] = {}
    for section in ps.xpath("./epo:Section", namespaces=NS):
        sname = section.get("name") or ""

        kv: dict[str, str] = {}
        for st in section.xpath("./epo:Setting", namespaces=NS):
            k = st.get("name") or ""
            v = st.get("value") or ""
            kv[k] = v

        sections[sname] = kv

    return {"meta": meta, "sections": sections}


# ------------------------------------------------------------------------------
# 5) NORMALIZATION: STABILIZE "LIST-LIKE" SETTINGS + IGNORE DERIVED COUNTS
# ------------------------------------------------------------------------------

# These are typical "list-like" patterns in your screenshots.
# Adjust/add as you discover more in your data.
LISTY_PREFIXES: tuple[str, ...] = (
    "ExcludedItem_",         # ExcludedItem_0..ExcludedItem_N (paths/patterns)
    "szApplicationItem_",    # application list entries
    "TypeItem_",             # numeric TypeItem_0.. entries
)

# These are often derived or redundant and can create noise if compared directly.
# You can keep them and validate consistency, but many teams just ignore them in diffs.
COUNT_NAMES: tuple[str, ...] = (
    "dwExclusionCount",
    "dwApp





##################################################### engine skecth

from __future__ import annotations
from dataclasses import dataclass
from lxml import etree
import re
from typing import Any

LISTY_PREFIXES = (
    "ExcludedItem_",
    "szApplicationItem_",
    "TypeItem_",
)

COUNT_NAMES = (
    "dwExclusionCount",
    "dwApplicationCount",
)

def _ns(root: etree._Element) -> dict[str, str]:
    # adapt as needed if prefix differs
    uri = root.nsmap.get("epo") or root.nsmap.get(None)
    return {"epo": uri} if uri else {}

def parse_settings_block(ps: etree._Element, NS: dict[str, str]) -> dict[str, Any]:
    meta = {k: ps.get(k) for k in ("name", "featureid", "categoryid", "typeid")}
    sections: dict[str, dict[str, str]] = {}

    for section in ps.xpath("./epo:Section", namespaces=NS):
        sname = section.get("name") or ""
        pairs = {}
        for st in section.xpath("./epo:Setting", namespaces=NS):
            k = st.get("name") or ""
            v = (st.get("value") or "").strip()
            pairs[k] = v
        sections[sname] = pairs

    return {"meta": meta, "sections": sections}

def normalize_settings(doc: dict[str, Any]) -> dict[str, Any]:
    """
    - Strip values
    - Convert list-like SettingName_0..N into a set under SettingName__LIST
    - Optionally ignore *_Count fields (or keep and validate consistency)
    """
    out = {"meta": doc["meta"], "sections": {}}

    for sname, kv in doc["sections"].items():
        norm: dict[str, Any] = {}
        lists: dict[str, set[str]] = {}

        for k, v in kv.items():
            # bucket list-like keys
            for pref in LISTY_PREFIXES:
                if k.startswith(pref):
                    lists.setdefault(pref, set()).add(v)
                    break
            else:
                # optionally ignore counts (they’re derived)
                if k in COUNT_NAMES:
                    # keep if you want; I usually drop and recompute
                    continue
                norm[k] = v

        # inject normalized lists
        for pref, items in lists.items():
            norm[f"{pref}__LIST"] = sorted(items)

        out["sections"][sname] = norm

    return out

def load_export(path: str) -> tuple[dict[str, Any], dict[tuple[str,str,str,str], list[str]]]:
    """
    Returns:
      - settings_by_name: EPOPolicySettings/@name -> normalized dict
      - objects: (objname, featureid, categoryid, typeid) -> list of referenced settings names
    """
    tree = etree.parse(path)
    root = tree.getroot()
    NS = _ns(root)

    settings_by_name: dict[str, Any] = {}
    for ps in root.xpath(".//epo:EPOPolicySettings", namespaces=NS):
        name = ps.get("name") or ""
        settings_by_name[name] = normalize_settings(parse_settings_block(ps, NS))

    objects: dict[tuple[str,str,str,str], list[str]] = {}
    for obj in root.xpath(".//epo:EPOPolicyObject", namespaces=NS):
        key = (
            obj.get("name") or "",
            obj.get("featureid") or "",
            obj.get("categoryid") or "",
            obj.get("typeid") or "",
        )
        refs = [x.text.strip() for x in obj.xpath("./epo:PolicySettings", namespaces=NS) if x.text]
        objects[key] = refs

    return settings_by_name, objects

def diff_dict(a: Any, b: Any, path: str = "") -> list[str]:
    """Tiny recursive diff (good enough for reporting)."""
    diffs: list[str] = []
    if type(a) != type(b):
        return [f"{path}: type {type(a).__name__} != {type(b).__name__}"]

    if isinstance(a, dict):
        keys = set(a) | set(b)
        for k in sorted(keys):
            p = f"{path}.{k}" if path else k
            if k not in a:
                diffs.append(f"{p}: missing in baseline, incoming={b[k]!r}")
            elif k not in b:
                diffs.append(f"{p}: removed in incoming, baseline={a[k]!r}")
            else:
                diffs.extend(diff_dict(a[k], b[k], p))
        return diffs

    if isinstance(a, list):
        if a != b:
            diffs.append(f"{path}: {a!r} != {b!r}")
        return diffs

    if a != b:
        diffs.append(f"{path}: {a!r} != {b!r}")
    return diffs

# Usage:
# base_settings, base_objects = load_export("baseline.xml")
# inc_settings, inc_objects = load_export("incoming.xml")
# Then for each policy object key, compare referenced settings blocks.



######################## SAVING TO DB PART


# =========================
# file: server_code/services/policies/ports.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Iterable, Sequence, ContextManager


@dataclass(frozen=True, slots=True)
class PolicyRow:
    policy_name: str
    policy_description: str | None
    policy_version: str
    policy_data: bytes
    policy_owner: str | None
    policy_status: str | None
    policy_metadata: str | None


class PolicyWriter(Protocol):
    def upsert_many(self, rows: Sequence[PolicyRow]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class PolicyWriterFactory(Protocol):
    def __call__(self) -> PolicyWriter: ...


# =========================
# file: server_code/services/trellix/http_client.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

import requests
from requests.auth import HTTPBasicAuth


class HttpSession(Protocol):
    def get(self, url: str, **kwargs): ...


@dataclass(frozen=True, slots=True)
class TrellixAuth:
    username: str
    password: str


@dataclass(frozen=True, slots=True)
class TrellixDownloadConfig:
    url: str
    auth: TrellixAuth
    verify_tls: bool = True
    timeout: tuple[float, float] = (10.0, 600.0)
    headers: Mapping[str, str] | None = None


def open_trellix_stream(
    *,
    session: HttpSession,
    cfg: TrellixDownloadConfig,
) -> requests.Response:
    headers: dict[str, str] = {"Accept": "application/xml"}
    if cfg.headers:
        headers.update(dict(cfg.headers))

    if not cfg.verify_tls:
        requests.packages.urllib3.disable_warnings(  # type: ignore[attr-defined]
            requests.packages.urllib3.exceptions.InsecureRequestWarning  # type: ignore[attr-defined]
        )

    resp = session.get(
        cfg.url,
        auth=HTTPBasicAuth(cfg.auth.username, cfg.auth.password),
        headers=headers,
        stream=True,
        verify=cfg.verify_tls,
        timeout=cfg.timeout,
    )
    resp.raise_for_status()
    resp.raw.decode_content = True
    return resp


# =========================
# file: server_code/services/trellix/xml_policy_stream.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Iterator

import xml.etree.ElementTree as ET

from services.policies.ports import PolicyRow


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _child_text_by_local(parent: ET.Element, local: str) -> str | None:
    for child in parent:
        if _local_name(child.tag) == local:
            if child.text is None:
                return None
            txt = child.text.strip()
            return txt if txt else None
    return None


def default_policy_mapper(policy_elem: ET.Element) -> PolicyRow:
    name = (
        _child_text_by_local(policy_elem, "PolicyName")
        or _child_text_by_local(policy_elem, "Name")
        or _child_text_by_local(policy_elem, "policy_name")
        or _child_text_by_local(policy_elem, "policyName")
    )
    if not name:
        name = f"__unnamed__:{id(policy_elem)}"

    version = (
        _child_text_by_local(policy_elem, "PolicyVersion")
        or _child_text_by_local(policy_elem, "Version")
        or _child_text_by_local(policy_elem, "policy_version")
        or _child_text_by_local(policy_elem, "policyVersion")
        or "1"
    )

    desc = (
        _child_text_by_local(policy_elem, "PolicyDescription")
        or _child_text_by_local(policy_elem, "Description")
        or _child_text_by_local(policy_elem, "policy_description")
        or _child_text_by_local(policy_elem, "policyDescription")
    )

    owner = (
        _child_text_by_local(policy_elem, "PolicyOwner")
        or _child_text_by_local(policy_elem, "Owner")
        or _child_text_by_local(policy_elem, "policy_owner")
        or _child_text_by_local(policy_elem, "policyOwner")
    )

    status = (
        _child_text_by_local(policy_elem, "PolicyStatus")
        or _child_text_by_local(policy_elem, "Status")
        or _child_text_by_local(policy_elem, "policy_status")
        or _child_text_by_local(policy_elem, "policyStatus")
    )

    metadata = (
        _child_text_by_local(policy_elem, "PolicyMetadata")
        or _child_text_by_local(policy_elem, "Metadata")
        or _child_text_by_local(policy_elem, "policy_metadata")
        or _child_text_by_local(policy_elem, "policyMetadata")
    )

    policy_bytes = ET.tostring(policy_elem, encoding="utf-8", method="xml")

    return PolicyRow(
        policy_name=name,
        policy_description=desc,
        policy_version=version,
        policy_data=policy_bytes,
        policy_owner=owner,
        policy_status=status,
        policy_metadata=metadata,
    )


@dataclass(frozen=True, slots=True)
class PolicyParseConfig:
    policy_element_local_name: str = "Policy"


def iter_policies_from_stream(
    *,
    stream,
    parse_cfg: PolicyParseConfig = PolicyParseConfig(),
    mapper: Callable[[ET.Element], PolicyRow] = default_policy_mapper,
) -> Iterator[PolicyRow]:
    context = ET.iterparse(stream, events=("end",))
    for event, elem in context:
        if _local_name(elem.tag) != parse_cfg.policy_element_local_name:
            continue
        row = mapper(elem)
        yield row
        elem.clear()


# =========================
# file: server_code/services/trellix/importer.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

import requests

from services.policies.ports import PolicyRow, PolicyWriterFactory
from services.trellix.http_client import HttpSession, TrellixDownloadConfig, open_trellix_stream
from services.trellix.xml_policy_stream import PolicyParseConfig, iter_policies_from_stream


@dataclass(frozen=True, slots=True)
class ImportConfig:
    batch_size: int = 200
    policy_element_local_name: str = "Policy"


def import_trellix_policies_to_db(
    *,
    session: HttpSession,
    download_cfg: TrellixDownloadConfig,
    writer_factory: PolicyWriterFactory,
    cfg: ImportConfig = ImportConfig(),
    mapper: Callable[[object], PolicyRow] | None = None,
) -> int:
    writer = writer_factory()
    written = 0
    batch: list[PolicyRow] = []

    try:
        resp = open_trellix_stream(session=session, cfg=download_cfg)
        with resp:
            parse_cfg = PolicyParseConfig(policy_element_local_name=cfg.policy_element_local_name)
            policy_iter = iter_policies_from_stream(
                stream=resp.raw,
                parse_cfg=parse_cfg,
                mapper=mapper if mapper is not None else None or __import__(
                    "services.trellix.xml_policy_stream",
                    fromlist=["default_policy_mapper"],
                ).default_policy_mapper,
            )

            for row in policy_iter:
                batch.append(row)
                if len(batch) >= cfg.batch_size:
                    writer.upsert_many(batch)
                    writer.flush()
                    written += len(batch)
                    batch.clear()

            if batch:
                writer.upsert_many(batch)
                writer.flush()
                written += len(batch)

        return written
    finally:
        writer.close()


# =========================
# OPTIONAL ADAPTER EXAMPLE (replace with your real DB code)
# file: server_code/services/policies/db_writer_example.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from services.policies.ports import PolicyRow, PolicyWriter


@dataclass(slots=True)
class InMemoryPolicyWriter(PolicyWriter):
    storage: dict[tuple[str, str], PolicyRow]

    def upsert_many(self, rows: Sequence[PolicyRow]) -> None:
        for r in rows:
            self.storage[(r.policy_name, r.policy_version)] = r

    def flush(self) -> None:
        return

    def close(self) -> None:
        return


def in_memory_writer_factory() -> InMemoryPolicyWriter:
    return InMemoryPolicyWriter(storage={})




##### TO imporve presenting differences incomparison scetch part

'''
Below is a drop-in addition that produces a ticket-friendly diff report:

grouped by Section

shows Added / Removed / Changed settings

for list-like settings (e.g. ExcludedItem__LIST, szApplicationItem__LIST) it shows added/removed list entries, not a giant before/after dump

optionally hides “noise keys” you don’t want operators to see

You can paste these functions into your existing file and call render_ticket_update(...).
'''

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ------------------------------------------------------------------------------
# Ticket-friendly diff model
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class FieldChange:
    section: str
    key: str
    change_type: str  # "added" | "removed" | "changed" | "list_added" | "list_removed"
    before: Any = None
    after: Any = None


def _is_list_key(key: str) -> bool:
    # matches our normalization convention: "<prefix>__LIST"
    return key.endswith("__LIST")


def diff_policy_settings_for_ticket(
    baseline: dict[str, Any],
    incoming: dict[str, Any],
    *,
    hide_keys: set[str] | None = None,
) -> list[FieldChange]:
    """
    Diff two *normalized* policy settings dicts (output of normalize_policy_settings()).

    Returns a list of FieldChange entries suitable for rendering in a ticket update.

    baseline/incoming structure expected:
      {
        "meta": {...},
        "sections": {
            "Default-Detection": { "bApplyNVP": "1", "TypeItem__LIST": [...], ... },
            ...
        }
      }

    hide_keys:
      optional set of keys to completely ignore in diff output.
    """
    hide_keys = hide_keys or set()

    base_sections: dict[str, dict[str, Any]] = baseline.get("sections", {})
    inc_sections: dict[str, dict[str, Any]] = incoming.get("sections", {})

    changes: list[FieldChange] = []

    all_sections = sorted(set(base_sections) | set(inc_sections))
    for section in all_sections:
        b = base_sections.get(section, {})
        i = inc_sections.get(section, {})

        # Section appears/disappears entirely
        if section not in base_sections:
            # Everything in incoming is "added"
            for key in sorted(i.keys()):
                if key in hide_keys:
                    continue
                if _is_list_key(key) and isinstance(i[key], list):
                    for entry in i[key]:
                        changes.append(FieldChange(section, key, "list_added", before=None, after=entry))
                else:
                    changes.append(FieldChange(section, key, "added", before=None, after=i[key]))
            continue

        if section not in inc_sections:
            # Everything in baseline is "removed"
            for key in sorted(b.keys()):
                if key in hide_keys:
                    continue
                if _is_list_key(key) and isinstance(b[key], list):
                    for entry in b[key]:
                        changes.append(FieldChange(section, key, "list_removed", before=entry, after=None))
                else:
                    changes.append(FieldChange(section, key, "removed", before=b[key], after=None))
            continue

        # Section exists in both: key-level diff
        all_keys = sorted(set(b) | set(i))
        for key in all_keys:
            if key in hide_keys:
                continue

            if key not in b:
                if _is_list_key(key) and isinstance(i[key], list):
                    for entry in i[key]:
                        changes.append(FieldChange(section, key, "list_added", before=None, after=entry))
                else:
                    changes.append(FieldChange(section, key, "added", before=None, after=i[key]))
                continue

            if key not in i:
                if _is_list_key(key) and isinstance(b[key], list):
                    for entry in b[key]:
                        changes.append(FieldChange(section, key, "list_removed", before=entry, after=None))
                else:
                    changes.append(FieldChange(section, key, "removed", before=b[key], after=None))
                continue

            # Both have the key
            bv = b[key]
            iv = i[key]

            if _is_list_key(key) and isinstance(bv, list) and isinstance(iv, list):
                # list semantic diff: show added/removed entries
                bset = set(bv)
                iset = set(iv)
                for entry in sorted(iset - bset):
                    changes.append(FieldChange(section, key, "list_added", before=None, after=entry))
                for entry in sorted(bset - iset):
                    changes.append(FieldChange(section, key, "list_removed", before=entry, after=None))
            else:
                if bv != iv:
                    changes.append(FieldChange(section, key, "changed", before=bv, after=iv))

    return changes


# ------------------------------------------------------------------------------
# Ticket rendering (human-friendly)
# ------------------------------------------------------------------------------

def _format_scalar(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, str):
        return v if v != "" else "(empty)"
    return repr(v)


def render_ticket_update(
    policy_label: str,
    changes: list[FieldChange],
    *,
    max_list_entries_per_section: int = 25,
) -> str:
    """
    Convert changes into a clean, readable text block for a ticket update.

    Output features:
    - grouped by Section
    - per-section: Changed keys, Added keys, Removed keys
    - list keys: show added/removed list entries (truncated per section)
    """
    if not changes:
        return f"Policy '{policy_label}': No differences detected vs baseline."

    # Group by section
    by_section: dict[str, list[FieldChange]] = {}
    for ch in changes:
        by_section.setdefault(ch.section, []).append(ch)

    lines: list[str] = []
    lines.append(f"Policy '{policy_label}': Drift detected vs baseline")
    lines.append("")

    for section in sorted(by_section.keys()):
        sec_changes = by_section[section]

        # Split by type
        changed = [c for c in sec_changes if c.change_type == "changed"]
        added = [c for c in sec_changes if c.change_type == "added"]
        removed = [c for c in sec_changes if c.change_type == "removed"]
        list_added = [c for c in sec_changes if c.change_type == "list_added"]
        list_removed = [c for c in sec_changes if c.change_type == "list_removed"]

        lines.append(f"[Section: {section}]")

        if changed:
            lines.append("  Changed:")
            for c in sorted(changed, key=lambda x: x.key):
                lines.append(f"    - {c.key}: {_format_scalar(c.before)}  ->  {_format_scalar(c.after)}")

        if added:
            lines.append("  Added:")
            for c in sorted(added, key=lambda x: x.key):
                lines.append(f"    - {c.key}: {_format_scalar(c.after)}")

        if removed:
            lines.append("  Removed:")
            for c in sorted(removed, key=lambda x: x.key):
                lines.append(f"    - {c.key}: {_format_scalar(c.before)}")

        # For list diffs, show entries rather than dumping whole lists
        if list_added or list_removed:
            # Group list changes by list key so it’s not noisy
            la_by_key: dict[str, list[str]] = {}
            lr_by_key: dict[str, list[str]] = {}
            for c in list_added:
                la_by_key.setdefault(c.key, []).append(str(c.after))
            for c in list_removed:
                lr_by_key.setdefault(c.key, []).append(str(c.before))

            for list_key in sorted(set(la_by_key) | set(lr_by_key)):
                added_entries = sorted(set(la_by_key.get(list_key, [])))
                removed_entries = sorted(set(lr_by_key.get(list_key, [])))

                lines.append(f"  List changes: {list_key}")

                if added_entries:
                    lines.append("    + Added entries:")
                    for entry in added_entries[:max_list_entries_per_section]:
                        lines.append(f"      + {entry}")
                    if len(added_entries) > max_list_entries_per_section:
                        lines.append(f"      ... +{len(added_entries) - max_list_entries_per_section} more")

                if removed_entries:
                    lines.append("    - Removed entries:")
                    for entry in removed_entries[:max_list_entries_per_section]:
                        lines.append(f"      - {entry}")
                    if len(removed_entries) > max_list_entries_per_section:
                        lines.append(f"      ... -{len(removed_entries) - max_list_entries_per_section} more")

        lines.append("")  # blank line between sections

    return "\n".join(lines).rstrip()


# ------------------------------------------------------------------------------
# Example integration point (inside your compare loop)
# ------------------------------------------------------------------------------

def build_ticket_text_for_settings_ref(
    policy_label: str,
    baseline_blob: dict[str, Any],
    incoming_blob: dict[str, Any],
) -> str:
    """
    Create the final ticket update text for one settings blob.
    """
    # If you want to hide some keys that are too noisy for operators, add here.
    # For example: hide_keys={"szProgExts"} or any module-specific keys you decide.
    hide_keys: set[str] = set()

    changes = diff_policy_settings_for_ticket(baseline_blob, incoming_blob, hide_keys=hide_keys)
    return render_ticket_update(policy_label, changes)


####################### adjust save to db part for mssql

'''
server_code/services/policies/mssql_writer.py
'''


from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Iterable

import pyodbc

from services.policies.ports import PolicyRow, PolicyWriter


@dataclass(slots=True)
class MsSqlPolicyWriter(PolicyWriter):
    conn: pyodbc.Connection
    table: str = "policies"
    commit_every_flush: bool = True

    def upsert_many(self, rows: Sequence[PolicyRow]) -> None:
        if not rows:
            return

        cur = self.conn.cursor()

        update_sql = f"""
        UPDATE {self.table}
        SET
            policy_description = ?,
            policy_data = ?,
            policy_owner = ?,
            policy_status = ?,
            policy_metadata = ?
        WHERE policy_name = ? AND policy_version = ?
        """

        update_params = [
            (
                r.policy_description,
                r.policy_data,
                r.policy_owner,
                r.policy_status,
                r.policy_metadata,
                r.policy_name,
                r.policy_version,
            )
            for r in rows
        ]

        cur.fast_executemany = True
        cur.executemany(update_sql, update_params)

        insert_sql = f"""
        INSERT INTO {self.table} (
            policy_name,
            policy_description,
            policy_version,
            policy_data,
            policy_owner,
            policy_status,
            policy_metadata
        )
        SELECT ?,?,?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM {self.table} WHERE policy_name = ? AND policy_version = ?
        )
        """

        insert_params = [
            (
                r.policy_name,
                r.policy_description,
                r.policy_version,
                r.policy_data,
                r.policy_owner,
                r.policy_status,
                r.policy_metadata,
                r.policy_name,
                r.policy_version,
            )
            for r in rows
        ]
        cur.executemany(insert_sql, insert_params)

    def flush(self) -> None:
        if self.commit_every_flush:
            self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.commit()
        finally:
            self.conn.close()
