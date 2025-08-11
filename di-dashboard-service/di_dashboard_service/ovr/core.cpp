from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import pandas as pd


# Functional core: stateless transformations and validations


@dataclass(frozen=True)
class ColumnMapping:
    # Map from source csv column -> target db column
    source_to_target: Dict[str, str]

    # Target column order for DB insert
    target_order: List[str]

    # Columns that should be treated as strings (ensure utf-8 normalization)
    string_columns: Optional[List[str]] = None


DEFAULT_TARGET_ORDER: List[str] = [
    "Path_ID",
    "Full_Path",
    "Directory_Structure",
    "Document_Name",
    "DFS",
    "Created_Date",
    "Modified_Date",
    "Accessed_Date",
    "creator_name",
    "Owner_Name",
    "Owner_Login",
    "Modifier_Name",
    "Modifier_Login",
    "Accessor_Name",
    "Accessor_Login",
    "Classify_Time",
    "Tags",
    "Ownership",
    "Inferred_Owner_Level",
    "Load_For",
]


def normalize_strings(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    if not columns:
        return df
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x.encode("utf-8").decode("utf-8") if isinstance(x, str) else x
            )
    return df


def compute_ownership(df: pd.DataFrame) -> pd.Series:
    owner = df.get("Owner_Login")
    modifier = df.get("Modifier_Login")
    accessor = df.get("Accessor_Login")
    # fillna chains work only on Series, ensure defaults exist
    if owner is None:
        owner = pd.Series([None] * len(df), index=df.index)
    if modifier is None:
        modifier = pd.Series([None] * len(df), index=df.index)
    if accessor is None:
        accessor = pd.Series([None] * len(df), index=df.index)
    return owner.fillna(modifier).fillna(accessor)


def transform_chunk(
    chunk: pd.DataFrame,
    mapping: ColumnMapping,
    load_for_value: str = "OVR",
) -> pd.DataFrame:
    """
    Transform an input chunk from the CSV into the DB-ready schema.

    - Renames columns according to mapping.source_to_target
    - Normalizes string columns
    - Computes Ownership
    - Adds Load_For and Inferred_Owner_Level
    - Reorders columns according to mapping.target_order (or default)
    """
    # 1) Rename columns from source -> target
    renamed = chunk.rename(columns=mapping.source_to_target)

    # 2) Normalize string columns if declared
    renamed = normalize_strings(renamed, mapping.string_columns or [])

    # 3) Ownership
    renamed["Ownership"] = compute_ownership(renamed)

    # 4) Additional columns
    renamed["Load_For"] = load_for_value
    if "Inferred_Owner_Level" not in renamed.columns:
        renamed["Inferred_Owner_Level"] = ""

    # 5) Column order
    target_order = mapping.target_order or DEFAULT_TARGET_ORDER
    # Some columns may be missing in input; select intersection then add missing as empty
    available = [c for c in target_order if c in renamed.columns]
    missing = [c for c in target_order if c not in renamed.columns]
    for m in missing:
        renamed[m] = ""  # ensure column exists, fill empty
    return renamed[target_order]
