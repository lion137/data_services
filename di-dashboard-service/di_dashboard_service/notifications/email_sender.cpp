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
            f"Connecting to SMTP server host={self.settings.host} port={self.settings.port} local_hostname={self.settings.local_hostname}"
        )
        smtp = smtplib.SMTP(
            host=self.settings.host,
            port=self.settings.port,
            local_hostname=self.settings.local_hostname,
            timeout=self.settings.timeout,
        )
        smtp.ehlo()
        return smtp

    def _build_message(
        self,
        subject: str,
        body: str,
        from_addr: str,
        to_addrs: List[str],
        html: bool,
        message_id: Optional[str] = None,
        header_to: Optional[str] = None,
    ) -> MIMEMultipart:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = header_to if header_to is not None else ", ".join(to_addrs)
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
        batch_size: int = 100,
        send_individual: bool = False,
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
        content_hash = hashlib.sha256((subject + "\n" + body).encode("utf-8")).hexdigest()[:12]

        sent: List[str] = []
        failed: Dict[str, str] = {}

        def _iter_chunks(items: List[str], size: int):
            if size <= 0:
                size = 100
            for i in range(0, len(items), size):
                yield items[i : i + size]

        try:
            with self._connect() as smtp:
                if send_individual:
                    for rcpt in to_list:
                        message_id = f"<{uuid.uuid4().hex}@email-sender>"
                        msg = self._build_message(subject, body, from_addr, [rcpt], html, message_id=message_id)
                        try:
                            result = smtp.sendmail(from_addr, [rcpt], msg.as_string())
                            if rcpt not in result:
                                sent.append(rcpt)
                                logger.info(
                                    f"Email sent: rcpt={rcpt} subject={subject} from={from_addr} msg_id={message_id} corr_id={correlation_id} body_sha={content_hash}"
                                )
                            else:
                                code, resp = result.get(rcpt, ("", ""))
                                failed[rcpt] = f"{code} {resp}"
                                logger.warning(
                                    f"Send failed: rcpt={rcpt} code={code} resp={resp} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                                )
                        except (smtplib.SMTPException, OSError, socket.error) as e:
                            failed[rcpt] = str(e)
                            logger.warning(
                                f"Send exception: rcpt={rcpt} error={e} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                            )
                else:
                    for chunk in _iter_chunks(to_list, batch_size):
                        message_id = f"<{uuid.uuid4().hex}@email-sender>"
                        msg = self._build_message(
                            subject, body, from_addr, chunk, html, message_id=message_id, header_to="undisclosed-recipients:;"
                        )
                        try:
                            result = smtp.sendmail(from_addr, chunk, msg.as_string())
                            initial_failures = {rcpt: f"{code} {resp}" for rcpt, (code, resp) in result.items()}
                            for rcpt in chunk:
                                if rcpt not in initial_failures:
                                    sent.append(rcpt)
                                    logger.info(
                                        f"Email sent: rcpt={rcpt} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                                    )
                            if initial_failures:
                                failed.update(initial_failures)
                                logger.warning(
                                    f"Batch had failures: count={len(initial_failures)} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                                )
                        except (smtplib.SMTPException, OSError, socket.error) as e:
                            # Mark entire chunk failed
                            for rcpt in chunk:
                                failed[rcpt] = str(e)
                            logger.exception(
                                f"Batch send exception: rcpt_count={len(chunk)} error={e} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                            )
        except (smtplib.SMTPException, OSError, socket.error) as e:
            logger.exception(
                f"SMTP connection failed: error={e} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
            )
            failed = {rcpt: str(e) for rcpt in to_list}

        if failed:
            to_retry = list(failed.keys())
            for rcpt in to_retry:
                err = failed.pop(rcpt)
                retry_msg_id = f"<{uuid.uuid4().hex}@email-sender>"
                ok = self._retry_single(rcpt, subject, body, from_addr, html, retry_msg_id, correlation_id, content_hash)
                if ok:
                    sent.append(rcpt)
                else:
                    failed[rcpt] = err if err else "send failed"

        logger.info(
            f"Email send summary: sent={len(sent)} failed={len(failed)} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
        )
        if failed:
            for r, e in failed.items():
                logger.error(
                    f"Failed recipient: rcpt={r} error={e} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                )
        return sent, failed

    def _retry_single(
        self,
        rcpt: str,
        subject: str,
        body: str,
        from_addr: str,
        html: bool,
        message_id: str,
        correlation_id: Optional[str],
        content_hash: str,
    ) -> bool:
        backoff = self.settings.retry_backoff
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                msg = self._build_message(subject, body, from_addr, [rcpt], html, message_id=message_id)
                with self._connect() as smtp:
                    result = smtp.sendmail(from_addr, [rcpt], msg.as_string())
                    if rcpt not in result:
                        logger.info(
                            f"Retry success: rcpt={rcpt} attempt={attempt} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                        )
                        return True
                    else:
                        code, resp = result.get(rcpt, ("", ""))
                        logger.warning(
                            f"Retry failed: rcpt={rcpt} attempt={attempt} code={code} resp={resp} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                        )
            except (smtplib.SMTPException, OSError, socket.error) as e:
                logger.warning(
                    f"Retry exception: rcpt={rcpt} attempt={attempt} error={e} subject={subject} from={from_addr} corr_id={correlation_id} body_sha={content_hash}"
                )
            if attempt < self.settings.max_retries:
                time.sleep(backoff)
                backoff *= 2
        return False
