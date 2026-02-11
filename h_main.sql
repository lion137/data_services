CREATE TABLE dbo.vault_tokens_prod (
    id                      BIGINT IDENTITY(1,1) PRIMARY KEY,

    token_name               NVARCHAR(200) NOT NULL,   -- logical name e.g. payments-service
    vault_addr               NVARCHAR(400) NOT NULL,   -- https://vault.prod.company

    -- Store token ENCRYPTED (not hashed), because you must use it to renew
    token_ciphertext         VARBINARY(MAX) NOT NULL,
    token_kid                NVARCHAR(100) NOT NULL,   -- key id/version used to encrypt
    token_accessor           NVARCHAR(256) NULL,       -- optional but useful for auditing

    created_at               DATETIME2(3) NOT NULL CONSTRAINT DF_vtp_created DEFAULT SYSUTCDATETIME(),
    last_renewed_at          DATETIME2(3) NULL,

    -- scheduling
    next_renew_at            DATETIME2(3) NOT NULL,
    expire_at                DATETIME2(3) NULL,        -- best-effort based on last lookup TTL
    last_seen_ttl_sec        INT NULL,
    renewable                BIT NULL,

    status                   NVARCHAR(20) NOT NULL CONSTRAINT DF_vtp_status DEFAULT N'ACTIVE',
    consecutive_failures     INT NOT NULL CONSTRAINT DF_vtp_fail DEFAULT 0,
    last_error               NVARCHAR(2000) NULL,

    -- locking for multi-worker safety
    locked_by                NVARCHAR(200) NULL,
    locked_until             DATETIME2(3) NULL,

    updated_at               DATETIME2(3) NOT NULL CONSTRAINT DF_vtp_updated DEFAULT SYSUTCDATETIME()
);

CREATE UNIQUE INDEX UX_vtp_token_name ON dbo.vault_tokens_prod(token_name);

CREATE INDEX IX_vtp_due ON dbo.vault_tokens_prod(status, next_renew_at)
INCLUDE (locked_until, locked_by);

CREATE INDEX IX_vtp_lock ON dbo.vault_tokens_prod(locked_until);


from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import hvac
import pyodbc  # typical for MSSQL

UTC = timezone.utc


class CryptoProvider:
    def decrypt(self, ciphertext: bytes, kid: str) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class Config:
    table_name: str                 # "dbo.vault_tokens_prod" or "dbo.vault_tokens_uat"
    worker_id: str = f"{socket.gethostname()}:{os.getpid()}"
    batch_size: int = 25
    lock_minutes: int = 10

    renew_fraction: float = 0.75
    min_renew_lead: timedelta = timedelta(hours=6)
    min_gap_between_renews: timedelta = timedelta(minutes=30)

    max_failures: int = 5


class VaultRenewerMSSQL:
    def __init__(self, cnxn: pyodbc.Connection, crypto: CryptoProvider, cfg: Config):
        self.cnxn = cnxn
        self.crypto = crypto
        self.cfg = cfg

    def run_once(self) -> int:
        now = self._utcnow()
        rows = self._claim_due(now)
        for r in rows:
            try:
                self._renew_one(r, now)
            except Exception as e:
                self._mark_failure(r["id"], f"Unhandled error: {type(e).__name__}: {e}", now)
        return len(rows)

    def _claim_due(self, now: datetime) -> List[Dict[str, Any]]:
        cur = self.cnxn.cursor()
        # Use server time for consistency in a multi-node environment
        sql = f"""
        DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
        DECLARE @lock_until DATETIME2(3) = DATEADD(MINUTE, ?, @now);

        ;WITH due AS (
            SELECT TOP (?) *
            FROM {self.cfg.table_name} WITH (READPAST, UPDLOCK, ROWLOCK)
            WHERE status = N'ACTIVE'
              AND next_renew_at <= @now
              AND (locked_until IS NULL OR locked_until <= @now)
            ORDER BY next_renew_at ASC
        )
        UPDATE due
        SET locked_by = ?,
            locked_until = @lock_until,
            updated_at = @now
        OUTPUT
            inserted.id,
            inserted.token_name,
            inserted.vault_addr,
            inserted.token_ciphertext,
            inserted.token_kid,
            inserted.token_accessor;
        """
        cur.execute(sql, self.cfg.lock_minutes, self.cfg.batch_size, self.cfg.worker_id)
        cols = [c[0] for c in cur.description]
        out = [dict(zip(cols, row)) for row in cur.fetchall()]
        self.cnxn.commit()
        return out

    def _renew_one(self, row: Dict[str, Any], now: datetime) -> None:
        token_plain = self.crypto.decrypt(row["token_ciphertext"], row["token_kid"])
        client = hvac.Client(url=row["vault_addr"], token=token_plain)

        lookup = client.auth.token.lookup_self()
        ttl = int(lookup["data"].get("ttl", 0) or 0)
        renewable = bool(lookup["data"].get("renewable", False))

        if ttl <= 0:
            self._mark_failure(row["id"], "lookup-self returned ttl<=0 (expired/revoked?)", now)
            return
        if not renewable:
            self._mark_failure(row["id"], "token not renewable (renewable=false)", now)
            return

        if self._should_renew(ttl):
            renew = client.auth.token.renew_self()
            lease = int(renew.get("auth", {}).get("lease_duration", 0) or 0)
            ttl = lease if lease > 0 else ttl
            did_renew = True
        else:
            did_renew = False

        next_renew_at, expire_at = self._compute_schedule(now, ttl)
        self._mark_ok(row["id"], now, ttl, renewable, next_renew_at, expire_at, did_renew)

    def _should_renew(self, ttl_seconds: int) -> bool:
        ttl = timedelta(seconds=ttl_seconds)

        # If remaining TTL already below lead window -> renew
        if ttl <= self.cfg.min_renew_lead:
            return True

        # Renew when remaining <= max((1-renew_fraction)*ttl, min_renew_lead)
        remaining_threshold = ttl * (1.0 - self.cfg.renew_fraction)
        return ttl <= max(remaining_threshold, self.cfg.min_renew_lead)

    def _compute_schedule(self, now: datetime, ttl_seconds: int) -> Tuple[datetime, datetime]:
        ttl = timedelta(seconds=ttl_seconds)
        expire_at = now + ttl

        candidate = now + (ttl * self.cfg.renew_fraction)
        latest_safe = expire_at - self.cfg.min_renew_lead
        if candidate > latest_safe:
            candidate = latest_safe

        soonest = now + self.cfg.min_gap_between_renews
        if candidate < soonest:
            candidate = soonest

        return candidate, expire_at

    def _mark_ok(
        self,
        token_id: int,
        now: datetime,
        ttl_seconds: int,
        renewable: bool,
        next_renew_at: datetime,
        expire_at: datetime,
        did_renew: bool,
    ) -> None:
        cur = self.cnxn.cursor()
        sql = f"""
        UPDATE {self.cfg.table_name}
        SET last_renewed_at = CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE last_renewed_at END,
            last_seen_ttl_sec = ?,
            renewable = ?,
            next_renew_at = ?,
            expire_at = ?,
            consecutive_failures = 0,
            last_error = NULL,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = SYSUTCDATETIME()
        WHERE id = ?;
        """
        cur.execute(
            sql,
            1 if did_renew else 0,
            ttl_seconds,
            1 if renewable else 0,
            next_renew_at,
            expire_at,
            token_id,
        )
        self.cnxn.commit()

    def _mark_failure(self, token_id: int, error: str, now: datetime) -> None:
        cur = self.cnxn.cursor()
        sql = f"""
        UPDATE {self.cfg.table_name}
        SET consecutive_failures = consecutive_failures + 1,
            last_error = ?,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = SYSUTCDATETIME(),
            status = CASE
              WHEN consecutive_failures + 1 >= ? THEN N'ERROR'
              ELSE status
            END
        WHERE id = ?;
        """
        cur.execute(sql, error[:2000], self.cfg.max_failures, token_id)
        self.cnxn.commit()

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(UTC)
