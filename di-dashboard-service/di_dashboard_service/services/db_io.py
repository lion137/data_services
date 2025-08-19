from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Sequence, Tuple, Dict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import config  # type: ignore
except Exception:  # pragma: no cover
    class _Cfg:
        mssql_conn = (
            "mysql+pymysql://dev:devpass@localhost:3306/DIDashboard"
            "?charset=utf8mb4&allow_public_key_retrieval=true"
        )
        env = "DEV"

    config = _Cfg()  # type: ignore

logger = logging.getLogger(__name__)


def _default_engine() -> Engine:
    return create_engine(f"{config.mssql_conn}")


def _chunked(seq: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    if size <= 0:
        size = 500
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def get_mails_to_send(
    dashboard: str,
    *,
    limit: Optional[int] = None,
    notification_type: Optional[str] = None,
    psid_whitelist: Optional[Sequence[str]] = None,
    engine_factory=_default_engine,
) -> List[str]:
    """
    Return distinct recipient emails pending notification for a given dashboard (Load_For).

    Parameters:
    - dashboard: value for DIRaw.Load_For (e.g., 'HR', 'OVR')
    - limit: optional TOP limit
    - notification_type: optional filter for UserNotification.NotificationType
    - psid_whitelist: optional list of PSIDs to filter FileOwnership.PSID IN (...)
    """
    top_clause = ""
    if limit is not None and limit > 0:
        top_clause = " top (:limit)"

    notif_clause = ""
    if notification_type:
        notif_clause = " and u.NotificationType = :notif"

    psid_clause = ""
    if psid_whitelist:
        # SQL Server parameter limit protection handled by chunking later if needed.
        psid_clause = " and fo.PSID in :psids"

    sql = text(
        f"""
        select distinct{top_clause} fo.OwnerEmail
        from FileOwnership fo
        join UserNotification u on u.PSID = fo.PSID
        join DIRaw d on d.Id = u.OwnershipId
        where u.Finished = '0'
          and fo.OwnerEmail is not null
          and d.Load_For = :dashboard{notif_clause}{psid_clause}
        """
    )

    params = {"dashboard": dashboard}
    if limit is not None and limit > 0:
        params["limit"] = limit
    if notification_type:
        params["notif"] = notification_type

    emails: List[str] = []
    engine = engine_factory()
    if psid_whitelist:
        for chunk in _chunked(list(psid_whitelist), 500):
            # SQLAlchemy requires tuples for IN params
            params_chunk = dict(params)
            params_chunk["psids"] = tuple(chunk)
            rows = engine.execute(sql, params_chunk)  # type: ignore
            emails.extend([r[0] for r in rows.fetchall()])
    else:
        rows = engine.execute(sql, params)  # type: ignore
        emails = [r[0] for r in rows.fetchall()]

    logger.info(f"Pending emails fetched: count={len(emails)} dashboard={dashboard} notif={notification_type}")
    return emails


def update_after_mails_send_bulk(
    dashboard: str,
    sent: Sequence[str],
    failed: Dict[str, str],
    *,
    notification_type: Optional[str] = None,
    engine_factory=_default_engine,
    in_chunk: int = 500,
) -> Tuple[int, int]:
    """
    Mark notifications finished in bulk for a given dashboard.

    Returns (updated_success, updated_error).
    """
    engine = engine_factory()

    ok_total = 0
    err_total = 0

    base_ok = (
        """
        update u
        set u.Finished = '1', u.IsError = '0'
        from UserNotification u
        join FileOwnership fo on fo.PSID = u.PSID
        join DIRaw d on d.Id = u.OwnershipId
        where u.Finished = '0'
          and d.Load_For = :dashboard
          and fo.OwnerEmail in :emails
        """
    )
    base_err = (
        """
        update u
        set u.Finished = '1', u.IsError = '1'
        from UserNotification u
        join FileOwnership fo on fo.PSID = u.PSID
        join DIRaw d on d.Id = u.OwnershipId
        where u.Finished = '0'
          and d.Load_For = :dashboard
          and fo.OwnerEmail in :emails
        """
    )
    if notification_type:
        base_ok += " and u.NotificationType = :notif"
        base_err += " and u.NotificationType = :notif"

    with engine.begin() as conn:  # transaction
        if sent:
            for chunk in _chunked(list(sent), in_chunk):
                rc = conn.execute(text(base_ok), {"dashboard": dashboard, "emails": tuple(chunk), "notif": notification_type}).rowcount  # type: ignore
                ok_total += int(rc or 0)
        if failed:
            fail_keys = list(failed.keys())
            for chunk in _chunked(fail_keys, in_chunk):
                rc = conn.execute(text(base_err), {"dashboard": dashboard, "emails": tuple(chunk), "notif": notification_type}).rowcount  # type: ignore
                err_total += int(rc or 0)

    logger.info(f"Bulk update complete: ok={ok_total} err={err_total} dashboard={dashboard} notif={notification_type}")
    return ok_total, err_total
