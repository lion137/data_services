from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from di_dashboard_service.ovr.processor import OVRProcessor, DEFAULT_MAPPING
from di_dashboard_service.services import db_io

logger = logging.getLogger(__name__)


def run_once() -> dict:
    """Run processor once. Returns summary dict for orchestration."""
    processor = OVRProcessor(mapping=DEFAULT_MAPPING)
    processed_zips = processor.process_all()
    rows_written = processor.rows_written
    logger.info(
        f"OVR run finished. processed_zips={processed_zips}, rows_written={rows_written}"
    )

    # Gate any downstream actions on whether data was written
    if rows_written == 0:
        logger.warning("No rows written to DIRaw; skipping downstream operations.")
        return {
            "processed_zips": processed_zips,
            "rows_written": rows_written,
            "downstream_ran": False,
        }

    # Run additional downstream functions immediately after data is saved
    try:
        if hasattr(db_io, "run_post_ingestion_tasks"):
            logger.info("Running post-ingestion tasks...")
            db_io.run_post_ingestion_tasks()  # type: ignore[attr-defined]
        else:
            logger.info("No post-ingestion tasks defined in db_io.run_post_ingestion_tasks; skipping.")
    except Exception as e:
        logger.exception(f"Post-ingestion tasks failed: {e}")

    return {
        "processed_zips": processed_zips,
        "rows_written": rows_written,
        "downstream_ran": True,
    }


def _email_notifications_job() -> None:
    try:
        if hasattr(db_io, "send_email_notifications"):
            logger.info("Running scheduled email notifications...")
            db_io.send_email_notifications()  # type: ignore[attr-defined]
        else:
            logger.info("No email job defined in db_io.send_email_notifications; skipping.")
    except Exception as e:
        logger.exception(f"Email notifications job failed: {e}")


def schedule() -> None:
    """
    Start a BlockingScheduler with two jobs (UTC timezone):
    - OVR run: every Tuesday at 05:43 UTC (07:43 CET/CEST approx.)
    - Email notifications: every Tuesday at 07:00 UTC (09:00 CET/CEST approx.)
    """
    scheduler = BlockingScheduler(timezone="UTC")

    # Run ingestion on Tuesday 05:43 UTC
    scheduler.add_job(
        run_once,
        CronTrigger(day_of_week="tue", hour=5, minute=43),
        id="ovr_tue_0543",
        replace_existing=True,
    )

    # Run email notifications on Tuesday 07:00 UTC
    scheduler.add_job(
        _email_notifications_job,
        CronTrigger(day_of_week="tue", hour=7, minute=0),
        id="emails_tue_0700",
        replace_existing=True,
    )

    logger.info("Starting OVR scheduler (UTC)...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("OVR scheduler stopped.")


if __name__ == "__main__":
    # schedule()
    run_once()  # debug
