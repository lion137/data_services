"""
MongoDB policy storage plan

Collections:
1. policy_versions
   Stores metadata per policy version and marks the latest version.

2. policy_views
   Stores frontend-ready policy data in chunks:
   transformed policies, application settings, exclusion settings, etc.

3. policy_changes
   Stores comparison/diff data against a baseline policy.

4. policy_raw
   Optional: stores raw translated MSSQL/module output for traceability/debugging.
"""

from datetime import datetime, timezone
from itertools import islice
from typing import Any

from pymongo import MongoClient, ReplaceOne, UpdateOne, ASCENDING, DESCENDING
from pymongo.database import Database


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "policies_db"

COL_POLICY_VERSIONS = "policy_versions"
COL_POLICY_VIEWS = "policy_views"
COL_POLICY_CHANGES = "policy_changes"
COL_POLICY_RAW = "policy_raw"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def chunked(items: list[dict[str, Any]], chunk_size: int = 500):
    iterator = iter(items)
    chunk_no = 0

    while True:
        batch = list(islice(iterator, chunk_size))
        if not batch:
            break

        yield chunk_no, batch
        chunk_no += 1


def get_db() -> Database:
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def setup_collections_and_indexes(db: Database) -> None:
    """
    Collections are auto-created by MongoDB on first insert,
    but creating them explicitly is clearer.
    """

    existing = db.list_collection_names()

    for collection_name in [
        COL_POLICY_VERSIONS,
        COL_POLICY_VIEWS,
        COL_POLICY_CHANGES,
        COL_POLICY_RAW,
    ]:
        if collection_name not in existing:
            db.create_collection(collection_name)

    db[COL_POLICY_VERSIONS].create_index(
        [("policy_name", ASCENDING), ("version", DESCENDING)],
        unique=True,
        name="uniq_policy_version",
    )

    db[COL_POLICY_VERSIONS].create_index(
        [("policy_name", ASCENDING), ("is_latest", ASCENDING)],
        name="latest_policy_lookup",
    )

    db[COL_POLICY_VIEWS].create_index(
        [
            ("policy_name", ASCENDING),
            ("version", DESCENDING),
            ("view_type", ASCENDING),
            ("chunk_no", ASCENDING),
        ],
        name="policy_view_chunks",
    )

    db[COL_POLICY_VIEWS].create_index(
        [
            ("policy_name", ASCENDING),
            ("version", DESCENDING),
            ("view_type", ASCENDING),
            ("section", ASCENDING),
            ("subcategory", ASCENDING),
        ],
        name="policy_view_section_lookup",
    )

    db[COL_POLICY_CHANGES].create_index(
        [
            ("policy_name", ASCENDING),
            ("version", DESCENDING),
            ("baseline_policy_name", ASCENDING),
            ("view_type", ASCENDING),
        ],
        name="policy_changes_lookup",
    )

    db[COL_POLICY_RAW].create_index(
        [("policy_name", ASCENDING), ("version", DESCENDING)],
        unique=True,
        name="uniq_raw_policy_version",
    )


def start_new_policy_version(
    db: Database,
    policy_name: str,
    version: int,
    source: str = "mssql_translation_pipeline",
) -> None:
    """
    Marks old versions as not latest and creates/updates metadata for the new version.
    """

    db[COL_POLICY_VERSIONS].update_many(
        {"policy_name": policy_name},
        {"$set": {"is_latest": False, "updated_at": utc_now()}},
    )

    db[COL_POLICY_VERSIONS].replace_one(
        {"policy_name": policy_name, "version": version},
        {
            "policy_name": policy_name,
            "version": version,
            "is_latest": True,
            "source": source,
            "imported_at": utc_now(),
            "updated_at": utc_now(),
            "views": {},
        },
        upsert=True,
    )


def save_policy_view_rows(
    db: Database,
    policy_name: str,
    version: int,
    view_type: str,
    rows: list[dict[str, Any]],
    chunk_size: int = 500,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """
    Saves frontend-ready rows in chunks.

    view_type examples:
    - transformed_policy
    - application_settings
    - exclusion_settings
    """

    extra_fields = extra_fields or {}

    db[COL_POLICY_VIEWS].delete_many(
        {
            "policy_name": policy_name,
            "version": version,
            "view_type": view_type,
        }
    )

    operations = []

    for chunk_no, batch in chunked(rows, chunk_size):
        doc_id = f"{policy_name}:v{version}:{view_type}:{chunk_no}"

        doc = {
            "_id": doc_id,
            "policy_name": policy_name,
            "version": version,
            "view_type": view_type,
            "chunk_no": chunk_no,
            "rows": batch,
            "row_count": len(batch),
            "updated_at": utc_now(),
            **extra_fields,
        }

        operations.append(
            ReplaceOne(
                {"_id": doc_id},
                doc,
                upsert=True,
            )
        )

    if operations:
        db[COL_POLICY_VIEWS].bulk_write(operations, ordered=False)

    db[COL_POLICY_VERSIONS].update_one(
        {"policy_name": policy_name, "version": version},
        {
            "$set": {
                f"views.{view_type}": {
                    "chunks": len(operations),
                    "rows": len(rows),
                    "chunk_size": chunk_size,
                },
                "updated_at": utc_now(),
            }
        },
    )


def save_policy_changes(
    db: Database,
    policy_name: str,
    version: int,
    baseline_policy_name: str,
    view_type: str,
    changes: list[dict[str, Any]],
) -> None:
    doc_id = f"{policy_name}:v{version}:baseline:{baseline_policy_name}:{view_type}"

    added = sum(1 for c in changes if c.get("change_type") == "added")
    removed = sum(1 for c in changes if c.get("change_type") == "removed")
    changed = sum(1 for c in changes if c.get("change_type") == "changed")

    db[COL_POLICY_CHANGES].replace_one(
        {"_id": doc_id},
        {
            "_id": doc_id,
            "policy_name": policy_name,
            "version": version,
            "baseline_policy_name": baseline_policy_name,
            "view_type": view_type,
            "summary": {
                "added": added,
                "removed": removed,
                "changed": changed,
                "total": len(changes),
            },
            "changes": changes,
            "updated_at": utc_now(),
        },
        upsert=True,
    )


def save_raw_policy(
    db: Database,
    policy_name: str,
    version: int,
    raw_data: dict[str, Any],
) -> None:
    db[COL_POLICY_RAW].replace_one(
        {"policy_name": policy_name, "version": version},
        {
            "_id": f"{policy_name}:v{version}:raw",
            "policy_name": policy_name,
            "version": version,
            "raw_data": raw_data,
            "updated_at": utc_now(),
        },
        upsert=True,
    )


def get_latest_version(db: Database, policy_name: str) -> int | None:
    doc = db[COL_POLICY_VERSIONS].find_one(
        {"policy_name": policy_name, "is_latest": True},
        {"_id": 0, "version": 1},
    )

    if not doc:
        return None

    return doc["version"]


def fetch_policy_view_chunk(
    db: Database,
    policy_name: str,
    view_type: str,
    chunk_no: int = 0,
    version: int | None = None,
) -> dict[str, Any] | None:
    if version is None:
        version = get_latest_version(db, policy_name)

    if version is None:
        return None

    return db[COL_POLICY_VIEWS].find_one(
        {
            "policy_name": policy_name,
            "version": version,
            "view_type": view_type,
            "chunk_no": chunk_no,
        }
    )


def fetch_all_policy_view_chunks(
    db: Database,
    policy_name: str,
    view_type: str,
    version: int | None = None,
) -> list[dict[str, Any]]:
    if version is None:
        version = get_latest_version(db, policy_name)

    if version is None:
        return []

    return list(
        db[COL_POLICY_VIEWS]
        .find(
            {
                "policy_name": policy_name,
                "version": version,
                "view_type": view_type,
            }
        )
        .sort("chunk_no", ASCENDING)
    )


def refresh_policy_example(db: Database) -> None:
    """
    Example full refresh for one policy version.
    Replace example rows with your translated MSSQL/module output.
    """

    policy_name = "GLOBAL_STD_SRV_ENS_TP_OAS"
    version = 4

    transformed_policy_rows = [
        {
            "section": "General",
            "subcategory": "On-Access Scan",
            "setting_name": "Enable On-Access Scan",
            "enabled": True,
            "value": "Enabled",
            "raw_value": 1,
        },
        {
            "section": "General",
            "subcategory": "Process Settings - What to Scan",
            "setting_name": "Boot Sectors",
            "enabled": True,
            "value": "Enabled",
            "raw_value": 1,
        },
    ]

    application_settings_rows = [
        {
            "file_process_name": "eudora.exe",
            "assigned_risk": "High Risk",
        },
        {
            "file_process_name": "nbftclnt.exe",
            "assigned_risk": "Low Risk",
        },
    ]

    exclusion_rows = [
        {
            "exclusion_type": "Default-Detection_Exclusions",
            "path_extension": r"C:\Program Files\Tanium\Tanium Client\\",
        },
        {
            "exclusion_type": "Default-Detection_Exclusions",
            "path_extension": r"C:\Program Files\VERITAS\NetBackup\\",
        },
    ]

    changes = [
        {
            "section": "General",
            "subcategory": "On-Access Scan",
            "setting_name": "Enable On-Access Scan",
            "baseline_value": False,
            "policy_value": True,
            "change_type": "changed",
        }
    ]

    raw_data = {
        "source": "example raw translated policy dict",
    }

    start_new_policy_version(db, policy_name, version)

    save_policy_view_rows(
        db=db,
        policy_name=policy_name,
        version=version,
        view_type="transformed_policy",
        rows=transformed_policy_rows,
        chunk_size=500,
    )

    save_policy_view_rows(
        db=db,
        policy_name=policy_name,
        version=version,
        view_type="application_settings",
        rows=application_settings_rows,
        chunk_size=500,
    )

    save_policy_view_rows(
        db=db,
        policy_name=policy_name,
        version=version,
        view_type="exclusion_settings",
        rows=exclusion_rows,
        chunk_size=500,
    )

    save_policy_changes(
        db=db,
        policy_name=policy_name,
        version=version,
        baseline_policy_name="BASELINE_POLICY",
        view_type="transformed_policy",
        changes=changes,
    )

    save_raw_policy(
        db=db,
        policy_name=policy_name,
        version=version,
        raw_data=raw_data,
    )


if __name__ == "__main__":
    db = get_db()
    setup_collections_and_indexes(db)
    refresh_policy_example(db)

    latest = get_latest_version(db, "GLOBAL_STD_SRV_ENS_TP_OAS")
    print("Latest version:", latest)

    chunk = fetch_policy_view_chunk(
        db=db,
        policy_name="GLOBAL_STD_SRV_ENS_TP_OAS",
        view_type="application_settings",
        chunk_no=0,
    )

    print(chunk)