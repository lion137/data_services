from __future__ import annotations

import logging
import smtplib
import socket
import time
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
import hashlib
import uuid
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


try:
    import config  # type: ignore
except Exception:
    class _Cfg:
        SMTP_HOST = "localhost"
        SMTP_PORT = 1025
        SMTP_FROM = "noreply@example.com"
        SMTP_TIMEOUT = 30
        SMTP_MAX_RETRIES = 3
        SMTP_RETRY_BACKOFF = 2.0  # seconds base
        SMTP_LOCAL_HOSTNAME: Optional[str] = None

    config = _Cfg()  # type: ignore


@dataclass(frozen=True)
class EmailSettings:
    host: str = getattr(config, "SMTP_HOST", "localhost")
    port: int = getattr(config, "SMTP_PORT", 25)
    from_addr: str = getattr(config, "SMTP_FROM", "noreply@example.com")
    timeout: int = getattr(config, "SMTP_TIMEOUT", 30)
    max_retries: int = getattr(config, "SMTP_MAX_RETRIES", 3)
    retry_backoff: float = getattr(config, "SMTP_RETRY_BACKOFF", 2.0)
    local_hostname: Optional[str] = getattr(config, "SMTP_LOCAL_HOSTNAME", None)


class EmailSender:
    """
    Send emails via SMTP with per-recipient failure
        reporting and retries.

    Returns (sent, failed) from send_bulk where:
      - sent: List[str] of recipients successfully sent
      - failed: Dict[str, str] mapping recipient -> error message
    """

    def __init__(self, settings: Optional[EmailSettings] = None) -> None:
        self.settings = settings or EmailSettings()

    def _connect(self) -> smtplib.SMTP:
        logger.debug(
            "Connecting to SMTP server host=%s port=%s local_hostname=%s",
            self.settings.host, self.settings.port, self.settings.local_hostname,
        )
        smtp = smtplib.SMTP(
            host=self.settings.host,
            port=self.settings.port,
            local_hostname=self.settings.local_hostname,
            timeout=self.settings.timeout,
        )
        smtp.ehlo()
        return smtp

    def _build_message(self, subject: str, body: str, from_addr: str, to_addrs: List[str], html: bool, message_id: Optional[str] = None) -> MIMEMultipart:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = message_id or make_msgid()
        if html:
            msg.attach(MIMEText(body, "html", _charset="utf-8"))
        else:
            msg.attach(MIMEText(body, "plain", _charset="utf-8"))
        return msg

    def send_bulk(
        self,
        recipients: Iterable[str],
        subject: str,
        body: str,
        *,
        html: bool = False,
        from_addr: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Tuple[List[str], Dict[str, str]]:
        """
        Send single message to multiple recipients.

        Strategy:
        1) Attempt a single send to all recipients; collect per-recipient
            failures from sendmail's return dict.
        2) Retry failed recipients individually with exponential
            backoff up to max_retries.
        """
        to_list = [r.strip() for r in recipients if r and r.strip()]
        if not to_list:
            logger.warning("send_bulk called with empty recipient list")
            return [], {}

        from_addr = from_addr or self.settings.from_addr
        message_id = f"<{uuid.uuid4().hex}@email-sender>"
        content_hash = hashlib.sha256((subject + "\n" + body).encode("utf-8")).hexdigest()[:12]
        msg = self._build_message(subject, body, from_addr, to_list, html, message_id=message_id)

        sent: List[str] = []
        failed: Dict[str, str] = {}

        try:
            with self._connect() as smtp:
                result = smtp.sendmail(from_addr, to_list, msg.as_string())
                initial_failures = {rcpt: f"{code} {resp}" for rcpt, (code, resp) in result.items()}
                for rcpt in to_list:
                    if rcpt not in initial_failures:
                        sent.append(rcpt)
                        logger.info(
                            "Email sent: rcpt=%s subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                            rcpt, subject, from_addr, message_id, correlation_id, content_hash,
                        )
                failed.update(initial_failures)
                if failed:
                    logger.warning(
                        "Initial send had failures: count=%d subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                        len(failed), subject, from_addr, message_id, correlation_id, content_hash,
                    )
        except (smtplib.SMTPException, OSError, socket.error) as e:
            logger.exception(
                "Batch send failed: error=%s subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                e, subject, from_addr, message_id, correlation_id, content_hash,
            )
            failed = {rcpt: str(e) for rcpt in to_list}

        if failed:
            to_retry = list(failed.keys())
            for rcpt in to_retry:
                err = failed.pop(rcpt)
                ok = self._retry_single(rcpt, subject, body, from_addr, html, message_id, correlation_id, content_hash)
                if ok:
                    sent.append(rcpt)
                else:
                    failed[rcpt] = err if err else "send failed"

        logger.info(
            "Email send summary: sent=%d failed=%d subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
            len(sent), len(failed), subject, from_addr, message_id, correlation_id, content_hash,
        )
        if failed:
            for r, e in failed.items():
                logger.error(
                    "Failed recipient: rcpt=%s error=%s subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                    r, e, subject, from_addr, message_id, correlation_id, content_hash,
                )
        return sent, failed

    def _retry_single(self, rcpt: str, subject: str, body: str, from_addr: str, html: bool, message_id: str, correlation_id: Optional[str], content_hash: str) -> bool:
        backoff = self.settings.retry_backoff
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                msg = self._build_message(subject, body, from_addr, [rcpt], html, message_id=message_id)
                with self._connect() as smtp:
                    result = smtp.sendmail(from_addr, [rcpt], msg.as_string())
                    if rcpt not in result:
                        logger.info(
                            "Retry success: rcpt=%s attempt=%d subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                            rcpt, attempt, subject, from_addr, message_id, correlation_id, content_hash,
                        )
                        return True
                    else:
                        code, resp = result.get(rcpt, ("", ""))
                        logger.warning(
                            "Retry failed: rcpt=%s attempt=%d code=%s resp=%s subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                            rcpt, attempt, code, resp, subject, from_addr, message_id, correlation_id, content_hash,
                        )
            except (smtplib.SMTPException, OSError, socket.error) as e:
                logger.warning(
                    "Retry exception: rcpt=%s attempt=%d error=%s subject=%s from=%s msg_id=%s corr_id=%s body_sha=%s",
                    rcpt, attempt, e, subject, from_addr, message_id, correlation_id, content_hash,
                )
            if attempt < self.settings.max_retries:
                time.sleep(backoff)
                backoff *= 2
        return False
