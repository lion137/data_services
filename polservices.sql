Perfect — yes: keeping a seen_policy_names: set[str] during streaming parse is exactly the clean way to detect:

new vs baseline: seen - baseline

missing vs baseline: baseline - seen

…and it removes the need for run_id columns entirely (for this baseline-check requirement). You’ll still want a transaction so “half-ingested” never commits.

Below is a complete continuation in the style you asked for:

One MSSQL table dbo.policies (latest snapshot only)

CQRS: queries.py + commands.py

Functional core: pure-ish normalization + hashing + diff decision

Imperative shell: downloader + lxml.iterparse() + DB transaction + baseline comparison

0) Folder layout (suggested)
src/
  core/
    policy_model.py
    policy_core.py
  infra/
    mssql/
      schema.py
      queries.py
      commands.py
      unit_of_work.py
    trellix/
      downloader.py
      parser.py
  app/
    ingest_policies.py

Everything in core/ should be unit-testable without DB/network.

1) MSSQL schema (one table)
src/infra/mssql/schema.py
SCHEMA_SQL = r"""
IF OBJECT_ID('dbo.policies', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.policies (
        policy_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

        -- Identity (full EPOPolicySettings/@name)
        policy_name NVARCHAR(512) NOT NULL UNIQUE,

        -- "Spec" fields (you can populate later; keep columns now)
        policy_description NVARCHAR(2000) NULL,
        policy_status NVARCHAR(50) NULL,     -- e.g. Enabled/Disabled
        policy_owner NVARCHAR(200) NULL,
        policy_metadata NVARCHAR(2000) NULL,

        -- Versioning (latest only)
        policy_version INT NOT NULL,
        changed_at DATETIME2(0) NOT NULL,
        created_at DATETIME2(0) NOT NULL,

        -- Settings snapshot
        policy_hash VARBINARY(32) NOT NULL,         -- SHA-256 of canonical JSON
        policy_data_json NVARCHAR(MAX) NOT NULL,    -- canonical JSON (sorted keys)
        policy_data_xml NVARCHAR(MAX) NULL          -- optional raw blob

        -- Add indexes as needed
    );

    CREATE INDEX IX_policies_hash ON dbo.policies(policy_hash);
END;
"""
2) CQRS: Queries / Commands + Unit of Work

We’ll avoid ORM. We’ll also avoid global connection state.

src/infra/mssql/unit_of_work.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Any, Optional

class DBConnection(Protocol):
    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...

class DBConnectionFactory(Protocol):
    def __call__(self) -> DBConnection: ...

@dataclass
class UnitOfWork:
    """
    Imperative shell object: manages 1 transaction.
    Not unit tested (per your preference).
    """
    conn_factory: DBConnectionFactory
    conn: Optional[DBConnection] = None

    def __enter__(self) -> "UnitOfWork":
        self.conn = self.conn_factory()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        assert self.conn is not None
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        finally:
            self.conn.close()
src/infra/mssql/queries.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Protocol, Any

class DBConnection(Protocol):
    def cursor(self) -> Any: ...

@dataclass(frozen=True)
class PolicyRow:
    policy_id: int
    policy_name: str
    policy_version: int
    policy_hash: bytes

def get_policy_by_name(conn: DBConnection, policy_name: str) -> Optional[PolicyRow]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT policy_id, policy_name, policy_version, policy_hash
        FROM dbo.policies
        WHERE policy_name = ?
        """,
        (policy_name,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return PolicyRow(
        policy_id=int(row[0]),
        policy_name=str(row[1]),
        policy_version=int(row[2]),
        policy_hash=bytes(row[3]),
    )

def list_all_policy_names(conn: DBConnection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT policy_name FROM dbo.policies")
    return {str(r[0]) for r in cur.fetchall()}

def list_baseline_policy_names(conn: DBConnection) -> set[str]:
    """
    If you want baseline = subset, simplest is a separate table dbo.baseline_policies.
    But you asked to keep it simple. Two options:

    A) Baseline is maintained in code/config file -> don’t query DB here.
    B) Baseline is a DB-maintained set -> create dbo.baseline_policies(policy_name PK).
    
    For now, we implement B. If you prefer A, remove this and inject baseline set.
    """
    cur = conn.cursor()
    cur.execute(
        """
        IF OBJECT_ID('dbo.baseline_policies', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.baseline_policies (
                policy_name NVARCHAR(512) NOT NULL PRIMARY KEY
            );
        END;
        """
    )
    cur.execute("SELECT policy_name FROM dbo.baseline_policies")
    return {str(r[0]) for r in cur.fetchall()}
src/infra/mssql/commands.py
from __future__ import annotations
from datetime import datetime
from typing import Protocol, Any, Optional

class DBConnection(Protocol):
    def cursor(self) -> Any: ...

def ensure_schema(conn: DBConnection, schema_sql: str) -> None:
    cur = conn.cursor()
    cur.execute(schema_sql)

def insert_policy(
    conn: DBConnection,
    *,
    policy_name: str,
    created_at: datetime,
    changed_at: datetime,
    policy_version: int,
    policy_hash: bytes,
    policy_data_json: str,
    policy_data_xml: Optional[str],
    policy_description: Optional[str] = None,
    policy_status: Optional[str] = None,
    policy_owner: Optional[str] = None,
    policy_metadata: Optional[str] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dbo.policies(
            policy_name,
            policy_description, policy_status, policy_owner, policy_metadata,
            policy_version, changed_at, created_at,
            policy_hash, policy_data_json, policy_data_xml
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            policy_name,
            policy_description, policy_status, policy_owner, policy_metadata,
            policy_version, changed_at, created_at,
            policy_hash, policy_data_json, policy_data_xml,
        ),
    )

def update_policy_snapshot(
    conn: DBConnection,
    *,
    policy_name: str,
    changed_at: datetime,
    new_version: int,
    policy_hash: bytes,
    policy_data_json: str,
    policy_data_xml: Optional[str],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE dbo.policies
        SET
            policy_version = ?,
            changed_at = ?,
            policy_hash = ?,
            policy_data_json = ?,
            policy_data_xml = ?
        WHERE policy_name = ?
        """,
        (
            new_version,
            changed_at,
            policy_hash,
            policy_data_json,
            policy_data_xml,
            policy_name,
        ),
    )
3) Functional core: canonicalization + hashing + change decision
src/core/policy_model.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Mapping

@dataclass(frozen=True)
class ParsedPolicy:
    policy_name: str  # full EPOPolicySettings/@name (identity)
    sections: Mapping[str, Mapping[str, str]]  # section -> setting -> value
    xml_blob: Optional[str] = None
src/core/policy_core.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Optional
import json
import hashlib

from .policy_model import ParsedPolicy

def to_canonical_dict(policy: ParsedPolicy) -> dict[str, Any]:
    """
    Pure function.
    Keep values EXACT (no trim, no lowercasing) per your requirement.
    Sort order is handled by JSON dump with sort_keys=True later.
    """
    # Defensive copy into plain dict (Mapping may be custom)
    sections: dict[str, dict[str, str]] = {
        sec_name: dict(settings)
        for sec_name, settings in policy.sections.items()
    }
    return {"policy_name": policy.policy_name, "sections": sections}

def canonical_json(obj: dict[str, Any]) -> str:
    """
    Stable JSON: sorted keys + compact separators.
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_bytes(text: str) -> bytes:
    return hashlib.sha256(text.encode("utf-8")).digest()

@dataclass(frozen=True)
class UpsertDecision:
    should_write: bool
    is_new_policy: bool
    new_version: int  # if should_write: version to save, else current
    reason: str

def decide_upsert(
    *,
    existing_version: Optional[int],
    existing_hash: Optional[bytes],
    new_hash: bytes,
) -> UpsertDecision:
    """
    Pure function:
    - No DB
    - No time
    """
    if existing_version is None or existing_hash is None:
        return UpsertDecision(
            should_write=True,
            is_new_policy=True,
            new_version=1,
            reason="new_policy",
        )
    if existing_hash != new_hash:
        return UpsertDecision(
            should_write=True,
            is_new_policy=False,
            new_version=existing_version + 1,
            reason="hash_changed",
        )
    return UpsertDecision(
        should_write=False,
        is_new_policy=False,
        new_version=existing_version,
        reason="no_change",
    )

This gives you deterministic behavior and easy unit tests:

same content → no write

changed setting → write version+1

4) Imperative shell: streaming download + iterparse yielding policies
src/infra/trellix/downloader.py
from __future__ import annotations
from typing import Iterator, Protocol
import requests

class BytesIterator(Protocol):
    def __iter__(self) -> Iterator[bytes]: ...

def stream_download(
    *,
    url: str,
    username: str,
    password: str,
    chunk_size: int = 1024 * 1024,
    timeout_s: int = 60,
    verify_tls: bool = True,
) -> Iterator[bytes]:
    """
    Not unit tested (shell).
    """
    with requests.get(
        url,
        auth=(username, password),
        stream=True,
        timeout=timeout_s,
        verify=verify_tls,
    ) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:  # skip keep-alive chunks
                yield chunk
src/infra/trellix/parser.py

This uses lxml.etree.iterparse with a file-like object. Easiest is to write chunks to a temp file (still streaming, doesn’t load memory), then iterparse that file. It’s also robust for 200MB.

from __future__ import annotations
from dataclasses import dataclass
from typing import Iterator, Optional
from lxml import etree
import tempfile
import os

from ...core.policy_model import ParsedPolicy

@dataclass(frozen=True)
class ParseOptions:
    keep_xml_blob: bool = True

def parse_policies_from_chunks(
    chunks: Iterator[bytes],
    *,
    options: ParseOptions = ParseOptions(),
) -> Iterator[ParsedPolicy]:
    """
    Shell: writes to temp file, then iterparse.
    Keeps memory low and works for large files.
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        for ch in chunks:
            tmp.write(ch)

    try:
        # Parse only EPOPolicySettings nodes
        context = etree.iterparse(
            tmp_path,
            events=("end",),
            tag="EPOPolicySettings",
            recover=False,
            huge_tree=True,
        )

        for event, elem in context:
            policy_name = elem.get("name")
            if not policy_name:
                elem.clear()
                continue

            sections: dict[str, dict[str, str]] = {}
            for sec in elem.findall("Section"):
                sec_name = sec.get("name") or ""
                settings: dict[str, str] = {}
                for setting in sec.findall("Setting"):
                    sname = setting.get("name")
                    if sname is None:
                        continue
                    # value may be absent -> treat as empty string or None?
                    # For precision, keep empty string if missing.
                    sval = setting.get("value")
                    settings[sname] = "" if sval is None else sval
                sections[sec_name] = settings

            xml_blob: Optional[str] = None
            if options.keep_xml_blob:
                xml_blob = etree.tostring(elem, encoding="unicode")

            yield ParsedPolicy(policy_name=policy_name, sections=sections, xml_blob=xml_blob)

            # Free memory held by lxml
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    finally:
        os.remove(tmp_path)
5) Orchestrator: ingestion with baseline comparison set (your requirement)

This is the place where we keep:

seen: set[str]

baseline: set[str] (injected from DB or config)

“notify admin” hooks (dummy functions injected)

src/app/ingest_policies.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol

from ..core.policy_core import (
    to_canonical_dict,
    canonical_json,
    sha256_bytes,
    decide_upsert,
)
from ..infra.mssql.unit_of_work import UnitOfWork, DBConnectionFactory
from ..infra.mssql import queries, commands
from ..infra.mssql.schema import SCHEMA_SQL
from ..infra.trellix.downloader import stream_download
from ..infra.trellix.parser import parse_policies_from_chunks, ParseOptions

class Clock(Protocol):
    def now_utc(self) -> datetime: ...

@dataclass(frozen=True)
class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

@dataclass(frozen=True)
class IngestConfig:
    keep_xml_blob: bool = True

@dataclass(frozen=True)
class DiffReport:
    new_policies: set[str]
    missing_policies: set[str]

def ingest_trellix_export(
    *,
    conn_factory: DBConnectionFactory,
    url: str,
    username: str,
    password: str,
    baseline_names_provider: Callable[[object], set[str]],  # takes conn
    notify_admin: Callable[[DiffReport], None],             # dummy for now
    clock: Clock = SystemClock(),
    config: IngestConfig = IngestConfig(),
) -> DiffReport:
    """
    Imperative shell:
    - Single transaction via UoW
    - Streaming download + streaming parse
    - In-memory set for baseline comparison (your requirement)
    """
    seen: set[str] = set()

    with UnitOfWork(conn_factory) as uow:
        assert uow.conn is not None
        conn = uow.conn

        # Ensure schema inside the same txn (or do it at startup separately)
        commands.ensure_schema(conn, SCHEMA_SQL)

        baseline_names = baseline_names_provider(conn)

        chunks = stream_download(url=url, username=username, password=password)
        policies = parse_policies_from_chunks(
            chunks,
            options=ParseOptions(keep_xml_blob=config.keep_xml_blob),
        )

        for parsed in policies:
            seen.add(parsed.policy_name)

            existing = queries.get_policy_by_name(conn, parsed.policy_name)

            canon = to_canonical_dict(parsed)
            canon_json = canonical_json(canon)
            h = sha256_bytes(canon_json)

            if existing is None:
                decision = decide_upsert(
                    existing_version=None,
                    existing_hash=None,
                    new_hash=h,
                )
                now = clock.now_utc().replace(tzinfo=None)
                commands.insert_policy(
                    conn,
                    policy_name=parsed.policy_name,
                    created_at=now,
                    changed_at=now,
                    policy_version=decision.new_version,
                    policy_hash=h,
                    policy_data_json=canon_json,
                    policy_data_xml=parsed.xml_blob,
                )
            else:
                decision = decide_upsert(
                    existing_version=existing.policy_version,
                    existing_hash=existing.policy_hash,
                    new_hash=h,
                )
                if decision.should_write:
                    now = clock.now_utc().replace(tzinfo=None)
                    commands.update_policy_snapshot(
                        conn,
                        policy_name=parsed.policy_name,
                        changed_at=now,
                        new_version=decision.new_version,
                        policy_hash=h,
                        policy_data_json=canon_json,
                        policy_data_xml=parsed.xml_blob,
                    )

        # end parse loop - now baseline diff
        new_policies = seen - baseline_names
        missing_policies = baseline_names - seen

        report = DiffReport(new_policies=new_policies, missing_policies=missing_policies)

        # Still inside txn, but notify_admin should ideally be outside.
        # For now: DO NOT send network/email in txn. Just return report.
        # Caller can notify after commit.
        return report
Caller usage (notify after commit)
def notify_admin_dummy(report):
    print("NEW:", len(report.new_policies))
    print("MISSING:", len(report.missing_policies))

def baseline_from_db(conn):
    return queries.list_baseline_policy_names(conn)

report = ingest_trellix_export(
    conn_factory=make_conn_factory(CONN_STR),
    url=TURL,
    username=USER,
    password=PASS,
    baseline_names_provider=baseline_from_db,
    notify_admin=notify_admin_dummy,
)
notify_admin_dummy(report)  # do it after function returns (after commit)

(Your UnitOfWork.__exit__ commits when no exception. So by the time ingest_trellix_export returns, DB is consistent.)

6) Why this satisfies your design constraints
CQRS

Reads: queries.py

Writes: commands.py

Functional core / imperative shell

Core: policy_core.py is pure logic (easy unit tests)

Shell: downloader.py, parser.py, ingest_policies.py (no unit tests required)

DI everywhere

conn_factory, baseline_names_provider, notify_admin, clock are injected

Core functions don’t touch external resources

No global mutable state

seen is per-run local variable

no module-level caches

7) One last practical note: baseline storage

You didn’t yet have a baseline example. You’ve got two good options:

Baseline set stored in DB table dbo.baseline_policies (what I stubbed)

Baseline stored as a file (JSON list of policy_name) loaded by shell and injected

Both integrate cleanly because baseline is injected via baseline_names_provider.

If you want, next step I can add:

a tiny script/command to seed dbo.baseline_policies from an export (e.g., “take the first successful run and mark all seen policies as baseline”), still using streaming + set.

please add

Absolutely — here’s the “baseline seeding” addition, keeping the same architecture (CQRS + functional-core/imperative-shell + DI). This lets you:

Seed baseline from an export (take the set of EPOPolicySettings/@name found in a file and store it as baseline)

Optionally replace baseline (truncate then insert)

Or merge (insert missing only)

I’ll add:

dbo.baseline_policies table into schema

queries.baseline_* reads

commands.baseline_* mutations

app/seed_baseline.py orchestrator using the same downloader+parser and in-memory set

1) Update schema to include baseline table
src/infra/mssql/schema.py
SCHEMA_SQL = r"""
IF OBJECT_ID('dbo.policies', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.policies (
        policy_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

        policy_name NVARCHAR(512) NOT NULL UNIQUE,

        policy_description NVARCHAR(2000) NULL,
        policy_status NVARCHAR(50) NULL,
        policy_owner NVARCHAR(200) NULL,
        policy_metadata NVARCHAR(2000) NULL,

        policy_version INT NOT NULL,
        changed_at DATETIME2(0) NOT NULL,
        created_at DATETIME2(0) NOT NULL,

        policy_hash VARBINARY(32) NOT NULL,
        policy_data_json NVARCHAR(MAX) NOT NULL,
        policy_data_xml NVARCHAR(MAX) NULL
    );

    CREATE INDEX IX_policies_hash ON dbo.policies(policy_hash);
END;

IF OBJECT_ID('dbo.baseline_policies', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.baseline_policies (
        policy_name NVARCHAR(512) NOT NULL PRIMARY KEY,
        created_at DATETIME2(0) NOT NULL
    );
END;
"""
2) Queries: baseline reads
src/infra/mssql/queries.py (add these)
from __future__ import annotations
from typing import Protocol, Any

class DBConnection(Protocol):
    def cursor(self) -> Any: ...

def list_baseline_policy_names(conn: DBConnection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT policy_name FROM dbo.baseline_policies")
    return {str(r[0]) for r in cur.fetchall()}

def baseline_count(conn: DBConnection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM dbo.baseline_policies")
    return int(cur.fetchone()[0])
3) Commands: baseline mutations
src/infra/mssql/commands.py (add these)
from __future__ import annotations
from datetime import datetime
from typing import Protocol, Any, Iterable

class DBConnection(Protocol):
    def cursor(self) -> Any: ...

def truncate_baseline(conn: DBConnection) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM dbo.baseline_policies")

def insert_baseline_names(
    conn: DBConnection,
    *,
    policy_names: Iterable[str],
    created_at: datetime,
    ignore_duplicates: bool = True,
) -> int:
    """
    Returns number of inserted rows (best effort).
    Uses a simple per-row insert to keep dependencies minimal.
    (Can be optimized later with TVP/bulk insert if needed.)
    """
    cur = conn.cursor()
    inserted = 0
    for name in policy_names:
        if ignore_duplicates:
            cur.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM dbo.baseline_policies WHERE policy_name = ?)
                BEGIN
                    INSERT INTO dbo.baseline_policies(policy_name, created_at) VALUES (?, ?)
                END
                """,
                (name, name, created_at),
            )
            # We can’t reliably count inserted rows without @@ROWCOUNT per statement,
            # but this is fine for now. If you want accurate count, we can fetch @@ROWCOUNT.
        else:
            cur.execute(
                "INSERT INTO dbo.baseline_policies(policy_name, created_at) VALUES (?, ?)",
                (name, created_at),
            )
        inserted += 1
    return inserted

(If you want accurate inserted count, tell me and I’ll switch to @@ROWCOUNT after each statement.)

4) Imperative shell: baseline seeder

This streams the export, collects policy names into a set, then writes baseline.

src/app/seed_baseline.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol, Literal

from ..infra.mssql.unit_of_work import UnitOfWork, DBConnectionFactory
from ..infra.mssql import commands, queries
from ..infra.mssql.schema import SCHEMA_SQL
from ..infra.trellix.downloader import stream_download
from ..infra.trellix.parser import parse_policies_from_chunks, ParseOptions

class Clock(Protocol):
    def now_utc(self) -> datetime: ...

@dataclass(frozen=True)
class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

BaselineMode = Literal["replace", "merge"]

@dataclass(frozen=True)
class SeedBaselineResult:
    mode: BaselineMode
    seen_count: int
    baseline_before: int
    baseline_after: int
    added_names: set[str]

def seed_baseline_from_export(
    *,
    conn_factory: DBConnectionFactory,
    url: str,
    username: str,
    password: str,
    mode: BaselineMode = "replace",   # replace or merge
    clock: Clock = SystemClock(),
) -> SeedBaselineResult:
    """
    Shell: streaming parse + DB write in one transaction.

    replace: baseline becomes exactly the set found in the export
    merge: baseline adds any missing entries but keeps existing ones
    """
    seen: set[str] = set()

    # 1) Stream download + parse -> seen names
    chunks = stream_download(url=url, username=username, password=password)
    for parsed in parse_policies_from_chunks(chunks, options=ParseOptions(keep_xml_blob=False)):
        seen.add(parsed.policy_name)

    now = clock.now_utc().replace(tzinfo=None)

    # 2) Write baseline inside one txn
    with UnitOfWork(conn_factory) as uow:
        assert uow.conn is not None
        conn = uow.conn

        commands.ensure_schema(conn, SCHEMA_SQL)

        baseline_before = queries.baseline_count(conn)

        if mode == "replace":
            commands.truncate_baseline(conn)
            commands.insert_baseline_names(conn, policy_names=sorted(seen), created_at=now, ignore_duplicates=False)
            added_names = seen  # effectively “baseline = seen”
        else:  # merge
            existing = queries.list_baseline_policy_names(conn)
            added_names = seen - existing
            if added_names:
                commands.insert_baseline_names(conn, policy_names=sorted(added_names), created_at=now, ignore_duplicates=True)

        baseline_after = queries.baseline_count(conn)

    return SeedBaselineResult(
        mode=mode,
        seen_count=len(seen),
        baseline_before=baseline_before,
        baseline_after=baseline_after,
        added_names=added_names,
    )
5) How to use it (example)
result = seed_baseline_from_export(
    conn_factory=make_conn_factory(CONN_STR),
    url=TURL,
    username=USER,
    password=PASS,
    mode="replace",  # or "merge"
)

print(result)
6) Integration with ingestion (baseline comparison using in-memory set)

Your ingestion function already keeps seen and compares to baseline_names. Now baseline is a real DB-maintained set, and “run_id” is unnecessary for missing/new checks.

If you want, I can also add a tiny “baseline sanity report” function (counts, sample missing/new) and a helper command to “promote current policies table into baseline” (i.e., baseline


https://chatgpt.com/share/699e40ba-7c68-8004-8e4e-70873303d5b2
########################NEW ####################################33
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

DATABASE_URL = (
    "mssql+pyodbc://username:password@server/database"
    "?driver=ODBC+Driver+18+for+SQL+Server"
)

# created once when module is imported
_engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    fast_executemany=True,  # speeds up bulk inserts for MSSQL
)

SessionLocal = sessionmaker(
    bind=_engine,
    autoflush=False,
    autocommit=False,
)


@contextmanager
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from .constants import INSERT_POLICY


type SessionProvider = Callable[[], AbstractContextManager[Session]]


def insert_policy(get_session: SessionProvider, policy: "TrellixPolicyRow") -> int:
    """
    Insert a single Trellix policy row into the database.

    Opens a new session using the provided session provider and executes the
    INSERT statement. Returns the number of inserted rows (1 on success).
    """
    params = _policy_params(policy)

    with get_session() as session:
        session.execute(text(INSERT_POLICY), params)

    return 1


def insert_policies(
    get_session: SessionProvider,
    policies: Iterable["TrellixPolicyRow"],
) -> int:
    """
    Insert multiple Trellix policy rows using a bulk operation.

    Executes the INSERT statement with a list of parameter mappings.
    Returns the number of rows attempted to be inserted. Returns 0 if
    the input iterable is empty.
    """
    rows = [_policy_params(policy) for policy in policies]
    if not rows:
        return 0

    with get_session() as session:
        session.execute(text(INSERT_POLICY), rows)

    return len(rows)


def _policy_params(policy: "TrellixPolicyRow") -> dict[str, Any]:
    """
    Convert a TrellixPolicyRow into a SQL parameter mapping for INSERT_POLICY.
    """
    return {
        "policy_name": policy.policy_name,
        "policy_description": policy.policy_description,
        "policy_status": policy.policy_status,
        "policy_owner": policy.policy_owner,
        "policy_metadata": policy.policy_metadata,
        "policy_version": policy.policy_version,
        "changed_at": policy.changed_at,
        "created_at": policy.created_at,
        "policy_hash": policy.policy_hash,
        "policy_data_json": policy.policy_data_json,
        "policy_data_xml": policy.policy_data_xml,
    }

# insert policies batched

def insert_policies(get_session, policies, chunk_size=500):
    batch = []
    inserted = 0

    with get_session() as session:
        for policy in policies:
            batch.append(_policy_params(policy))

            if len(batch) >= chunk_size:
                session.execute(text(INSERT_POLICY), batch)
                inserted += len(batch)
                batch.clear()

        if batch:
            session.execute(text(INSERT_POLICY), batch)
            inserted += len(batch)

    return inserted

# usage 

from sqlalchemy.exc import SQLAlchemyError

from .trellix_policy_repo import insert_policies


def persist_policies(get_session, policies) -> int:
    try:
        return insert_policies(get_session, policies)
    except SQLAlchemyError as exc:
        raise RuntimeError("Failed to persist Trellix policies") from exc



# sql exampel 

INSERT INTO policies (
    policy_name,
    policy_description,
    policy_status,
    policy_owner,
    policy_metadata,
    policy_version,
    changed_at,
    created_at,
    policy_hash,
    policy_data_json,
    policy_data_xml
)
VALUES (
    :policy_name,
    :policy_description,
    :policy_status,
    :policy_owner,
    :policy_metadata,
    :policy_version,
    :changed_at,
    :created_at,
    :policy_hash,
    :policy_data_json,
    :policy_data_xml
)
services/
    database/
        session.py
        trellix/
            commands.py
            queries.py
            sql.py
            types.py