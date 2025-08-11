from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .processor import OVRProcessor, DEFAULT_MAPPING

logger = logging.getLogger(__name__)


def run_once() -> int:
    """Run processor once, return processed zip count."""
    processor = OVRProcessor(mapping=DEFAULT_MAPPING)
    count = processor.process_all()
    logger.info(f"OVR run finished. processed_zips={count}")
    return count


def schedule(cron: str = "*/15 * * * *") -> None:
    """
    Start a BlockingScheduler to run OVR processing on a cron schedule.
    Default: every 15 minutes. Use standard 5-field cron format (min hour dom mon dow).
    """
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run_once, CronTrigger.from_crontab(cron), id="ovr_job", replace_existing=True)

    logger.info("Starting OVR scheduler...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("OVR scheduler stopped.")


if __name__ == "__main__":
    # Allow running directly: python -m di_dashboard_service.ovr.scheduler
    schedule()
