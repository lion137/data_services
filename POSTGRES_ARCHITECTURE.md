# Policy Portal: Migrating MongoDB document storage to PostgreSQL

**Architecture discussion, proposed v1 schema, storage/versioning strategy, JSONB querying, and Anvil connections**  
**Prepared:** 2026-10-23  
**Companion file:** [`sql.py`](sql.py)  
**Status:** design proposal for review and benchmarking; **not** an approved production migration

> **Scope.** This document consolidates our discussion of PostgreSQL as a document database and applies it to the Policy Portal and its CrowdStrike policies and exclusions. The examples are intentionally illustrative. The screenshots expose only portions of the existing application and collections; schema assumptions below must be checked against actual data, queries, and business rules before production use.

## Executive recommendation

Use PostgreSQL as a **hybrid relational/document store**, not as a MongoDB-shaped database with a single JSONB column and not as a fully normalized representation of every policy setting:

1. **Relational columns** for stable external identity, tenant scope, revision identity, platform, source version, important timestamps, status, and content hashes.
2. **JSONB** for heterogeneous configuration settings, vendor-specific metadata, and complex exclusion attributes.
3. **Immutable revisions** with explicit history. Keep `observed`, `baseline`, and `draft`/edited revisions logically distinct until their business semantics are confirmed.
4. **Changes as append-only rows** (`operation`, `json_pointer`, `old_value`, `new_value`), not one continuously updated JSONB array of edits.
5. **Snapshots plus optional deltas**: begin with complete snapshots for correctness and add sparse revisions/checkpoints after benchmarking. A snapshot/delta schema is provided, but patch application is deliberately not implemented in `sql.py`.
6. **Hosts and usage separately** from large immutable policy documents; usage is an observation workload, not configuration itself.
7. **Small, bounded Psycopg connection pools per Anvil server interpreter**, with connection metrics; add PgBouncer only if observed connection growth justifies it.

The main technical risk is not whether PostgreSQL can handle nested JSON—it can. The risks are **version correctness, semantic equivalence during migration, write amplification, reconstruction complexity, and unnecessary database connections**.

---

## 1. What we know from the MongoDB screenshots

The current Portal contains collections with names including:

| MongoDB collection | Current logical role | Proposed PostgreSQL destination |
|---|---|---|
| `crowdstrike_policies` | Observed configurations | `policy`, `policy_revision` (`observed` stream), `policy_state` |
| `crowdstrike_baseline_policies` | Baseline configurations | `policy_revision` (`baseline` stream), `policy_state` |
| `crowdstrike_exclusions` | Observed exclusions | `exclusion`, `exclusion_revision`, `exclusion_state` |
| `crowdstrike_baseline_exclusions` | Exclusion baselines | `exclusion_revision` (`baseline` stream) |
| `crowdstrike_policy_usage` | Usage/host snapshots | `policy_usage_snapshot`, optional `policy_usage_host` |
| `crowdstrike_audit_log` | Audit records | `audit_event` |

Other Trellix collections also appear in Robo 3T. **They are not modeled in this CrowdStrike-focused v1 proposal**, though the principles should generalize. `source_system` exists on identity tables so the design need not depend on one vendor.

### Fields visible in sample policy documents

Stable identity and searchable metadata: `policy_id`, `policy_type`, `cid`, `policy_name`, `platform`, `policy_version`, `created_at`, `created_by`, `modified_at`, `modified_by`, `source`, `saved_at`, `saved_by`, `settings_hash`.

Operational/derived state: `active`, `is_active`, `is_disabled`, `hosts_status`, `baseline_status`, `edit_revision`, `changes_vs_baseline_json`, `owner`.

Nested/variable payloads: `settings`, `policy_metadata`, `os_versions`, `host_group_ids`, `hosts`.

Example setting structures differ by type:

```json
{
  "setting_id": "ScriptBasedExecutionMonitoring",
  "category": "Enhanced Visibility",
  "type": "toggle",
  "value": {"configured": true, "enabled": false}
}
```

```json
{
  "setting_id": "CloudAntiMalware",
  "type": "mlslider",
  "value": {"detection": "AGGRESSIVE", "prevention": "MODERATE"}
}
```

This heterogeneity argues against a huge network of setting-specific relational tables. Exclusions have a different shape (e.g. `kind`, `exclusion_id`, `attributes.regexp_value`, `attributes.value_hash`, `host_groups`, `applied_globally`, `exclusion_hash`). Their identities and history can be relational while their specific content remains JSONB.

**Important observation:** `policy_metadata` in one screenshot appears to be *JSON encoded inside a string*. A migration must not silently treat a JSONB string as an object. Choose and document one canonical representation, and preserve original bytes separately if exact source-text fidelity is legally or operationally required.

### Existing Python layers

The screenshots show domain objects such as `CrowdStrikePolicyRow`, `CrowdStrikeExclusionRow`, and `CrowdStrikePolicyUsage`, plus functions `policy_to_doc`, `policy_from_doc`, `exclusion_to_doc`, `exclusion_from_doc`, `usage_to_doc`, `usage_from_doc`, and `_stamp`. These are a useful separation point. Preserve the domain-facing API and refactor repositories/query implementations instead of propagating PostgreSQL details into Anvil UI code.

---

## 2. PostgreSQL fundamentals relevant to this migration

### 2.1 Relational engine, transactional behavior

PostgreSQL is an object-relational database with native `jsonb`, arrays, rich types, extensible indexing, SQL joins, transactions, and constraints. It offers ACID transactions; the application must still enforce business-specific invariants.

A policy revision insert, patch/event inserts, audit event, and update of the current pointer should form **one database transaction**. If a step fails, the transaction rolls back. For concurrent editors, use an **expected/current revision check** while holding the appropriate row lock or a conditional update with a checked affected-row count; a plain `SELECT` followed by an unguarded `UPDATE` is insufficient.

### 2.2 MVCC is not business history

PostgreSQL employs multi-version concurrency control (MVCC). An ordinary `UPDATE` creates a new heap tuple (row version). Old physical versions can later be reclaimed by VACUUM; they are **not** a permanent audit/history feature. Business history requires explicit immutable revision records.

PostgreSQL's usual default isolation is **READ COMMITTED**. A carefully implemented expected-revision check can serialize competing edits; SERIALIZABLE is another option but requires retry logic for serialization failures. Use the least complex correct transaction strategy.

### 2.3 WAL, durability, replication

Write-ahead logging (WAL) supports crash recovery and physical streaming replication. Heavy JSONB updates can increase WAL production. A standby replica is not an independent backup; backups, WAL archiving, retention and restore drills are required. A multi-site high-availability installation additionally needs promotion/failover orchestration and careful split-brain prevention.

### 2.4 Process-per-connection and pooling

A PostgreSQL connection has a real server-side resource cost. One pool per Anvil interpreter is **not** one pool per application deployment. Bound each pool carefully; distinguish checked-out, idle, and `idle in transaction` connections.

---

## 3. `json` versus `jsonb`

| Behavior | `json` | `jsonb` |
|---|---|---|
| Internal form | Original JSON text | Parsed binary representation |
| Key order | Preserved in text | Not preserved |
| Duplicate object keys | Preserved in text | Only final key value retained |
| GIN indexing | Not directly | Supported |
| Typical Portal use | Source-text preservation, if needed | Configuration payloads and JSON querying |

**Prefer JSONB for application payloads.** It supports containment, key/path operators, JSONPath, and suitable GIN indexes. Do not expect converting BSON → Python dict → JSONB → Python dict → JSON text to preserve the original byte representation.

Distinguish:

- SQL `NULL` in a column;
- JSON `null` within a JSONB value (`'null'::jsonb`);
- a missing object property.

Also be deliberate about numeric conversion, array order, duplicate setting IDs, and canonical hashing. A `settings_hash` based on serialized bytes needs a documented Python canonicalization algorithm independent of database output formatting.

---

## 4. Why JSONB update behavior determines our storage design

### 4.1 An `UPDATE` makes a new heap tuple

For example, given `(is_active boolean, payload jsonb)`, running:

```sql
UPDATE policy_portal.policy_state
SET is_active = false, updated_at = now()
WHERE policy_pk = 123 AND stream = 'draft';
```

creates a new row version in that *small state table*; no large JSONB payload resides there.

If an unchanged large JSONB attribute in the *same row* is stored out-of-line with TOAST, PostgreSQL can often reuse its existing TOAST value when some other column changes. Thus **new row version does not necessarily mean full duplication of every unchanged large attribute**.

### 4.2 Updating a nested JSONB property still creates a new JSONB value

Example of a direct JSONB modification:

```sql
UPDATE example_table
SET payload = jsonb_set(
  payload,
  '{settings,network_visibility,enabled}',
  'false'::jsonb
)
WHERE id = 123;
```

Logically this changes one Boolean. Physically the modified JSONB value is reconstructed, a new heap tuple is written, and an out-of-line value may require new TOAST storage. Relevant indexes and WAL must also be considered. **PostgreSQL does not normally patch a few bytes in place inside stored JSONB.**

### 4.3 TOAST and compression

Large values may be compressed and stored out-of-line in TOAST chunks. Compression often works well on repetitive structured data, but incurs CPU and does not provide general *cross-row* deduplication. Identical sections in successive revision documents are not automatically stored once and referenced as deltas.

### 4.4 HOT updates

Heap-Only Tuple (HOT) updates can avoid new index entries when indexed values do not change (subject to index rules and space availability). The small `policy_state` table is well suited to this approach if volatile fields are not over-indexed. Putting those same values inside a GIN-indexed JSONB payload tends to be less favorable for updates.

### 4.5 Costs to measure

Do not optimize using JSON text length alone. Measure **heap size, TOAST size, indexes, WAL, compression ratio, update frequency, reconstructed read latency and query plans**. Large immutable documents and large frequently rewritten documents are very different workloads.

---

## 5. Proposed logical model

```text
policy (stable source identity, unique per CID/source/type)
  ├── policy_revision [observed | baseline | draft]
  │     ├── full JSONB snapshot OR delta-only revision
  │     └── policy_revision_change (ordered edit operations)
  ├── policy_state (one current revision pointer per stream)
  └── policy_usage_snapshot
        └── policy_usage_host (optional detailed hosts)

exclusion (stable source identity, unique per CID/source/kind)
  ├── exclusion_revision [observed | baseline | draft]
  │     ├── full JSONB snapshot OR delta-only revision
  │     └── exclusion_revision_change
  └── exclusion_state

audit_event (append-only audit log)
```

This yields **11 tables** in the v1 bootstrap. It intentionally does *not* create a table for every policy setting, host group, or vendor-specific structure.

### 5.1 Stable identity tables

`policy`:

- Internal `policy_pk bigint identity` primary key.
- `cid`, `source_system`, `policy_type`, `source_policy_id`.
- Unique identity combination `(cid, source_system, policy_type, source_policy_id)`.

`exclusion`: similar, keyed naturally by `(cid, source_system, kind, source_exclusion_id)`.

These uniqueness scopes are **hypotheses**. Confirm the real CrowdStrike API identity domain, multi-CID behavior and multi-vendor normalization first. The MongoDB `ObjectId` can be carried temporarily in an import mapping, but should not become the sole business identity.

### 5.2 `policy_revision` and `exclusion_revision`

Both represent a **linear revision sequence per stream**:

- `stream`: `observed`, `baseline`, or `draft`.
- `revision_no`: **portal-internal** monotonic integer, starting at 1.
- `source_policy_version` / `source_exclusion_version`: external version, independent of the portal sequence.
- `previous_revision_no`: predecessor in the same identity/stream.
- `storage_mode`: `snapshot` or `delta`.
- `document jsonb`: populated for snapshots; NULL for delta-only revisions.
- `canonical_hash`: canonical logical reconstructed-content hash (if implemented).
- Source, author and timestamps.

The schema requires revision 1 to be a full snapshot; later revisions may be full checkpoints or deltas. Composite foreign keys enforce the predecessor's identity/stream. They **do not** validate patch semantics or prove that a delta revision has an operation row—that must be done in a transactional repository or eventually by a trigger.

A snapshot should be a complete, independently reconstructible version of the **chosen canonical configuration model**, not necessarily a verbatim dump of every volatile MongoDB field.

### 5.3 Current state tables

`policy_state` / `exclusion_state` include current revision references, active/disabled status, last observed timestamp, baseline status and update timestamp. They can be updated cheaply without modifying immutable configuration blobs.

**Temporal distinction:** a historical `baseline_status` may belong on the revision/event that observed it; the latest status belongs in current state. Confirm exact meanings. The bootstrap is a starting point, not a complete temporal model.

### 5.4 Exclusions

Keep complex exclusion attributes and the historical host-group descriptors in JSONB. Promote `applied_globally`, `host_group_ids` and the exclusion hash to relational/native-array fields only where querying or integrity benefits. Native `text[]` identifiers can use GIN. If you need strict foreign keys and independently managed host groups, add normalized group identity/association tables in a later migration.

### 5.5 Host and usage data

`policy_usage_snapshot` stores observation timestamps, state/counters, group IDs and an extensible JSONB metrics object. `policy_usage_host` optionally contains per-device details keyed by `(usage_pk, device_id)`.

**Be cautious:** storing every host in every periodic usage snapshot may dwarf the entire policy-settings dataset. Consider retention limits, changes-only host history, or a separate current-assignment table after assessing actual volumes. Also distinguish group membership, source-reported assignment and effective applied policy.

### 5.6 Audit log

`audit_event` is append-only. Keep the ability to audit application actions independently of delta patches. A patch tells **what changed**; an audit event can additionally record **who requested/approved it, why, and through which workflow**.

### 5.7 What's intentionally absent from v1

- No full-text search or document-wide GIN index on every `document`.
- No automatic patch engine, rollback/merge logic or checkpoint scheduler.
- No Trellix-specific schema, approval-workflow tables, host-group master table, or row-level security policy.
- No automatic conversion of MongoDB ObjectIds, BSON timestamps or JSON-encoded strings.
- No automatic migration of existing production data.

These require domain validation and workload measurements first.

---

## 6. Full snapshots, deltas, and checkpoints

Three storage options:

| Design | Advantages | Drawbacks |
|---|---|---|
| Full snapshot per revision | Simple recovery, indexing, fast historical reads | Duplicated payload per version |
| Initial snapshot + every delta | Very compact for sparse changes | Reconstruction becomes expensive; searching effective history is hard |
| **Periodic snapshots + deltas** | Trade-off between compactness and bounded reconstruction | More implementation complexity |

Illustrative arithmetic, *not a PostgreSQL capacity forecast*: 100 versions of a 200-KiB document consume 19.5 MiB of raw full snapshots. One 200-KiB snapshot plus 99 patches of 500 bytes consumes ~244 KiB before row/index overhead, compression and metadata. Actual savings must be measured because snapshots can compress well and patches are not free.

### 6.1 Reconstruction model

Let `D_n` be the canonical document at revision `n`, and `Δ_n` the ordered patch set:

```text
D_n = apply(D_(n-1), Δ_n)
```

For revision `n`, find the newest checkpoint `c <= n`, load `D_c`, then apply `Δ_(c+1), ..., Δ_n`. Verify `canonical_hash`, if defined. If a patch's expected old value does not match the reconstructed document, **fail loudly rather than silently rebuilding an incorrect historical policy**.

An adaptive checkpoint policy can use:

- Number of patches since checkpoint;
- cumulative serialized/compressed patch bytes relative to snapshot bytes;
- measured reconstruction time;
- edit complexity (e.g., extensive array reorder);
- high-frequency reads of the current version.

### 6.2 What to store for each edit

The accompanying schema stores one row per operation:

```json
{
  "revision": 12,
  "operation_no": 1,
  "operation": "replace",
  "json_pointer": "/settings/CloudAntiMalware/value/prevention",
  "setting_id": "CloudAntiMalware",
  "old_present": true,
  "old_value": "MODERATE",
  "new_present": true,
  "new_value": "CAUTIOUS"
}
```

This is a **domain patch representation inspired by JSON Patch**. The replay contract, preconditions, path semantics, array behavior and JSON null handling must be defined and tested. `old_present/new_present` distinguish absent from explicit JSON `null`. `add` might legally replace an existing path in standard RFC 6902, so do not claim strict RFC compatibility without implementing all applicable semantics.

**Do not** append every edit to a single ever-growing JSONB array: that means rewriting the history value on every change.

### 6.3 Stable setting IDs versus array offsets

Source JSON stores settings as an array. Pointer `/settings/2/value/enabled` is fragile if arrays are reordered. A canonical internal mapping keyed by `setting_id` supports stable edit addressing:

```json
{
  "settings": {
    "CloudAntiMalware": {
      "type": "mlslider",
      "value": {"detection": "AGGRESSIVE", "prevention": "MODERATE"}
    }
  }
}
```

This is only safe if IDs are unique within the intended scope and ordering/duplicates have no meaningful source semantics. An alternative is a domain-aware patch containing `setting_id` and a resolver that locates it in the canonical array.

### 6.4 Concurrency

If two editors read revision 7, one must not overwrite the other's work. The save command should carry `expected_revision=7`, and the transaction must verify it against the current-state row before inserting revision 8 and moving the pointer. On mismatch: reject, refresh, or apply a defined merge policy. `revision_no` must be unique per identity/stream. Create revision, patch rows, audit and state update atomically.

### 6.5 Recommendation for rollout

**Begin by writing complete immutable snapshots for every saved revision, plus structured edit/audit events.** This minimizes migration and debugging risk. After benchmarks demonstrate material storage savings, write some *new* revisions as deltas and create checkpoints. The provided schema supports both; application code should not assume every revision row has a populated document.

---

## 7. PostgreSQL JSONB queries — working concepts and examples

These are educational queries, not finalized production query APIs. Assume `policy_portal.policy_revision.document` contains a full snapshot. Delta-only rows have `document IS NULL` and must be reconstructed first (or addressed through an indexed materialized/current representation).

### Extracting a property: `->`, `->>`, `#>`, `#>>`

```sql
-- JSONB result versus text result
SELECT
    document -> 'settings' AS settings_json,
    document ->> 'policy_name' AS policy_name_text
FROM policy_portal.policy_revision
WHERE storage_mode = 'snapshot';

-- Nested path: #> returns JSONB, #>> returns text
SELECT document #>> '{metadata,platform}'
FROM policy_portal.policy_revision
WHERE storage_mode = 'snapshot';
```

The exact path depends on the canonical payload chosen during migration. Fields promoted to relational columns should generally be read from those columns.

### Expanding a JSONB array

```sql
SELECT p.policy_pk,
       s.element ->> 'setting_id' AS setting_id,
       s.element -> 'value' AS setting_value
FROM policy_portal.policy_revision AS p
CROSS JOIN LATERAL jsonb_array_elements(p.document -> 'settings')
    AS s(element)
WHERE p.storage_mode = 'snapshot';
```

This can be useful for exports/analysis but can be expensive across many large documents. Array expansion is unnecessary for basic list screens.

### Containment

```sql
SELECT revision_pk, policy_pk
FROM policy_portal.policy_revision
WHERE storage_mode = 'snapshot'
  AND document @> '{
    "settings": [
      {"setting_id": "EndUserNotifications", "value": {"enabled": false}}
    ]
  }'::jsonb;
```

Containment matches a nested object within an array regardless of its position. Whether it benefits from GIN depends on index/operator choice, plan and selectivity.

### JSONPath

```sql
SELECT revision_pk
FROM policy_portal.policy_revision
WHERE document @? '$.settings[*] ? (@.setting_id == "CloudAntiMalware" && @.value.detection == "AGGRESSIVE")';
```

The `@?`/`@@` operators have specific GIN-supported patterns; arbitrary JSONPath function use is **not** automatically indexed.

### Small value manipulation

```sql
SELECT jsonb_set(
    '{"options":{"network":true}}'::jsonb,
    '{options,network}',
    'false'::jsonb
);

SELECT '{"options":{"network":true,"filesystem":false}}'::jsonb
       #- '{options,filesystem}';
```

These are functional transformations, **not in-place JSONB mutations**. For stable historical revisions, record a new version rather than updating old version content.

### Index choices

- **B-tree**: source identity, `policy_pk`, stream and version, dates, platform, statuses.
- **Composite B-tree**: current/latest records, ordered history; an ascending B-tree can be scanned backward, so avoid duplicate descending indexes where unnecessary.
- **GIN `jsonb_ops`**: broader JSONB key existence/containment and supported JSONPath operators.
- **GIN `jsonb_path_ops`**: usually smaller/more selective for containment and supported JSONPath, but lacks the existence operators.
- **Expression index**: for frequently queried nested scalars when canonical paths are stable.
- **GIN on `text[]`**: if host-group membership searches on native arrays become important.
- **BRIN**: potentially later for large, append-only, physically time-correlated histories.

The bootstrap deliberately omits a large generic JSONB GIN index. Add it only after inspecting actual queries with `EXPLAIN (ANALYZE, BUFFERS)` and measuring index size and insert overhead. An actual `EXPLAIN ANALYZE` **executes** the statement.

---

## 8. Idempotent PostgreSQL DDL in `sql.py`

### 8.1 Is `CREATE TABLE IF NOT EXISTS` supported?

**Yes**. PostgreSQL supports:

```sql
CREATE SCHEMA IF NOT EXISTS policy_portal;
CREATE TABLE IF NOT EXISTS policy_portal.example (...);
CREATE INDEX IF NOT EXISTS ix_example ON policy_portal.example (...);
```

An existing object is left alone. **Critical limitation:** PostgreSQL does not guarantee that an existing object has the desired columns, constraints, or index definition. Thus a rerunnable initializer is not a replacement for migrations.

### 8.2 What's in the companion module?

- All 11 `CREATE TABLE IF NOT EXISTS` statements and initial indexes;
- primary keys, unique business identities, foreign keys, basic type/content checks;
- separate revision streams and snapshot/delta modes;
- an `apply_schema(conn)` function;
- one transaction per bootstrap run (via the Psycopg connection context);
- a transaction-scoped advisory lock to serialize concurrent initializers;
- **no drops, no data import, no patch replay and no hard-coded credentials**.

The code is targeted at **PostgreSQL 14+ and Psycopg 3**. The most important design limit: constraints cannot generally verify “this delta revision has at least one well-formed change” across rows. Write that transactionally in the repository or add a validated trigger once the edit contract is final.

### 8.3 How to run

Install the driver:

```bash
pip install "psycopg[binary]>=3.1"
```

On Linux/macOS:

```bash
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DBNAME'
python sql.py
```

On Windows PowerShell:

```powershell
$env:DATABASE_URL = 'postgresql://USER:PASSWORD@HOST:5432/DBNAME'
python .\sql.py
```

These are placeholders: prefer protected secrets, and avoid committing connection strings to Git. Use an account authorized to create schemas/tables. **Run schema bootstrap as a deployment/admin step**, not on every Anvil server call. The generated script has not been executed against a live PostgreSQL server here; validate on a disposable PostgreSQL instance before production deployment.

### 8.4 What idempotence does *not* mean

- Re-running does not backfill new columns.
- Re-running does not change existing indexes whose names already exist.
- Re-running does not migrate old MongoDB documents.
- Re-running does not repair schema drift.
- Existing tables with conflicting definitions might cause errors when dependent objects are created, rather than being magically corrected.

For future releases use **Alembic** or a similar versioned migration framework. Use migration numbering, review migrations, and backfill carefully. In production, long index creation may need `CREATE INDEX CONCURRENTLY`, which has special transaction restrictions and is deliberately not used in this initial transactional bootstrap.

---

## 9. Anvil + PostgreSQL: pools, sessions and transaction scope

### 9.1 Critical distinction

- **Connection**: client socket/server backend resource.
- **Session**: server state on that connection.
- **Transaction**: bounded atomic unit of work *within* a connection.
- **Pool**: a bounded collection of reusable connections.

Anvil server calls may run in distinct/recreated Python interpreters depending on the environment and persistent-server-module settings. A module-global pool is **per interpreter/process**, not application-wide. Three pools each allowing four connections can expose twelve PostgreSQL sessions.

### 9.2 Suggested initial pool configuration

Use Psycopg 3 + `psycopg_pool`, **lazy per interpreter**. Example—not a guaranteed ideal production size:

```python
import threading
from psycopg_pool import ConnectionPool

_pool = None
_lock = threading.Lock()


def get_pool():
    global _pool
    with _lock:
        if _pool is None or _pool.closed:
            _pool = ConnectionPool(
                conninfo=get_postgres_dsn_from_anvil_secrets(),
                min_size=0,
                max_size=3,
                max_idle=60,
                max_lifetime=1800,
                timeout=5,
                open=True,
            )
        return _pool
```

The getter name is intentionally a placeholder—you need to implement it with server-side Anvil Secrets. `min_size=0` avoids forcing permanent minimum idle connections. A pool `max_size=3` limits *one instance*, not all Anvil interpreters across dev/UAT/prod/background workers.

### 9.3 Short transaction pattern

```python

def read_policy_identity(policy_pk: int):
    with get_pool().connection() as conn:
        row = conn.execute(
            "SELECT source_policy_id, policy_type "
            "FROM policy_portal.policy WHERE policy_pk = %s",
            (policy_pk,),
        ).fetchone()
        return row
```

The pool context returns the connection. On normal exit it commits any active transaction; on error it rolls back. Use parameterized SQL. **Do not hold the connection while the browser user edits a form, while calling the remote CrowdStrike API, or while waiting for external services**, unless a short, explicitly bounded transactional step demands it.

### 9.4 Monitoring

```sql
SELECT application_name, usename, state, count(*) AS sessions
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY application_name, usename, state
ORDER BY sessions DESC;
```

Assign names such as `policy-portal-dev`, `policy-portal-uat`, `policy-portal-prod`, and `policy-portal-worker`. In Psycopg inspect `pool.get_stats()`. Watch waiting clients, checkouts, total sessions and `idle in transaction` age. The correct pool size depends on concurrency and query duration, not just number of UI users.

### 9.5 PgBouncer

Consider PgBouncer if many Anvil processes create too many largely idle backend sessions. Transaction pooling is efficient for short work but can break assumptions about per-session features. Test prepared statements, session settings, temporary tables and other session-dependent behavior with your selected driver and PgBouncer versions.

---

## 10. Migration execution strategy

1. **Inventory Mongo queries/writes** in `collections.py`, `repository.py`, `queries.py`, `documents.py`, plus Anvil server call paths, background tasks and scheduled ingestion. Capture actual find/sort/update semantics and indexes.
2. **Profile current data**: document bytes, percentiles, settings counts, number of revisions, host counts, rate of edits/observations, tenant identity collisions.
3. **Define canonical serialization** (JSON null/missing, sorting, arrays, timestamps, BSON/ObjectId, source metadata and hashes). Test Python round-trips.
4. **Validate the relational schema** and scope of source IDs. Resolve whether baseline, observation, draft, approval and portal edits are separate lifecycle streams.
5. **Run `sql.py` in a disposable PostgreSQL database**; verify keys, FKs, JSONB checks and transactional inserts, then test rerun. Transition to Alembic for evolution.
6. **Refactor repositories** behind domain-facing interfaces; keep UI and domain comparisons as stable as possible. Psycopg 3 direct SQL is sufficient initially; SQLAlchemy is optional rather than mandatory.
7. **Migrate representative data and compare** document reconstruction, hashes, settings, exclusions, host groups, status and timestamps. Audit unsupported/null/malformed source cases.
8. **Benchmark** full snapshots against patch/checkpoint approaches under realistic reads, edits, exports and host observations.
9. **Plan cutover**: write freeze or reliable change capture, final reconciliation, backup/restore test, rollback decision and verification of read/write consistency.

### 10.1 Acceptance tests worth writing early

- Same identity within a CID/source/type maps to the same policy; identities in different CIDs do not collide.
- Revision 1 cannot be stored as a delta.
- A new revision must point to its actual predecessor.
- Concurrent edits based on the same expected revision cannot both silently succeed.
- Applying deltas produces the exact canonical document and expected hash.
- An explicit JSON null is distinguished from a missing key.
- Settings reordered in source do not cause accidental wrong-path edits.
- Large host inventories do not inflate configuration revision data.
- PostgreSQL historical lookup and list queries meet target latency.
- Re-running `sql.py` on an unchanged installation succeeds without deleting data.

---

## 11. Design questions still requiring stakeholder decisions

These are decisions, not missing syntax:

**A. What counts as a policy revision?** New CrowdStrike source version, user edit, baseline update and periodic observation are not necessarily the same event. The schema keeps external version distinct from portal revision number.

**B. Does every portal edit create a new immutable revision?** Recommended yes; an uncommitted editing session may instead be transient application state.

**C. Do we need exact source bytes?** If yes, preserve raw source independently from canonical JSONB. JSONB changes lexical representation.

**D. What must be queryable across historical versions?** Effective setting values, current snapshots, change events and last-modified timestamps lead to different indexes/materializations.

**E. What host history must be preserved?** Periodic full membership copies can be expensive. A current table plus changes or periodically summarized observations may be preferable.

**F. Which constraints are tenant-scoped?** `cid` may be enough for CrowdStrike but needs verification for Trellix and any multi-tenant identity model.

**G. Where do approvals, ownership and edit status live?** These may justify workflow/state tables separate from configuration revisions.

**H. Is storage cost really the primary pressure?** If documents are small and revision counts limited, snapshot simplicity may be worth more than saving relatively little disk space.

---

## 12. References (official documentation)

- PostgreSQL JSON and JSONB: <https://www.postgresql.org/docs/current/datatype-json.html>
- JSON functions and operators: <https://www.postgresql.org/docs/current/functions-json.html>
- MVCC introduction: <https://www.postgresql.org/docs/current/mvcc-intro.html>
- TOAST internals: <https://www.postgresql.org/docs/current/storage-toast.html>
- HOT updates: <https://www.postgresql.org/docs/current/storage-hot.html>
- CREATE TABLE (`IF NOT EXISTS` limitations): <https://www.postgresql.org/docs/current/sql-createtable.html>
- CREATE INDEX (`IF NOT EXISTS` limitations): <https://www.postgresql.org/docs/current/sql-createindex.html>
- EXPLAIN and optimizer: <https://www.postgresql.org/docs/current/using-explain.html>
- Psycopg 3 connection pool: <https://www.psycopg.org/psycopg3/docs/api/pool.html>
- JSON Patch RFC 6902: <https://www.rfc-editor.org/rfc/rfc6902>

**Bottom line:** PostgreSQL is fully capable of supporting the Portal's variable policy documents. The principal engineering choices are how to represent versions, where to place frequently changing state, how to limit duplicate data without compromising historic reconstruction, and how to keep Anvil's database access predictable. Use the provided `sql.py` as an inspectable **v1 bootstrap proposal**, not as an implicit approval of every domain assumption.
