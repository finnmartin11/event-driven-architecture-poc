from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from traceback import format_exc
from typing import Any

from dotenv import load_dotenv

from polling.repository import PollingPocEvent, PollingPocRepository
from utilities import create_db_engine


@dataclass(frozen=True)
class PollingWorkerConfig:
    worker_id: str
    polling_interval_seconds: float
    max_polling_interval_seconds: float
    simulated_processing_seconds: float
    stale_processing_reset_minutes: int


def process_rows(rows: list[dict[str, Any]], simulated_processing_seconds: float) -> None:
    """
    Replace this with real POC processing if needed.

    For the first benchmark, it is useful to keep processing intentionally
    simple so the measured difference is mostly polling latency.
    """
    if simulated_processing_seconds > 0:
        time.sleep(simulated_processing_seconds)

    logging.info("Processed %s raw rows", len(rows))


def calculate_backoff(
    current_sleep_seconds: float,
    base_sleep_seconds: float,
    max_sleep_seconds: float,
) -> float:
    return min(
        max(current_sleep_seconds * 2, base_sleep_seconds),
        max_sleep_seconds,
    )


def process_event(
    repository: PollingPocRepository,
    event: PollingPocEvent,
    config: PollingWorkerConfig,
) -> None:
    event_id = event.polling_poc_event_queue_id

    logging.info(
        "Claimed event_id=%s raw_id_range=%s-%s row_count=%s",
        event_id,
        event.min_raw_data_id,
        event.max_raw_data_id,
        event.row_count,
    )

    try:
        rows = repository.fetch_raw_rows_for_event(event)

        process_rows(
            rows=rows,
            simulated_processing_seconds=config.simulated_processing_seconds,
        )

        repository.mark_event_processed(event_id)

        logging.info("Marked event_id=%s processed", event_id)

    except Exception:
        msg = format_exc()
        logging.error("Failed processing event_id=%s\n%s", event_id, msg)
        repository.mark_event_failed(event_id, msg)


def run_polling_worker(
    repository: PollingPocRepository,
    config: PollingWorkerConfig,
) -> None:
    logging.info("Starting polling worker: %s", config.worker_id)

    sleep_time = config.polling_interval_seconds

    reset_count = repository.reset_stale_processing_events(
        stale_after_minutes=config.stale_processing_reset_minutes,
    )
    logging.info("Reset %s stale processing events", reset_count)

    while True:
        try:
            event = repository.claim_next_event(config.worker_id)

            if event is None:
                logging.info("No unprocessed event found. Sleeping %.3fs", sleep_time)
                time.sleep(sleep_time)

                sleep_time = calculate_backoff(
                    current_sleep_seconds=sleep_time,
                    base_sleep_seconds=config.polling_interval_seconds,
                    max_sleep_seconds=config.max_polling_interval_seconds,
                )
                continue

            process_event(
                repository=repository,
                event=event,
                config=config,
            )

            sleep_time = config.polling_interval_seconds

        except KeyboardInterrupt:
            logging.info("Stopping polling worker")
            return

        except Exception:
            logging.exception("Unexpected polling worker error")
            time.sleep(sleep_time)


def load_config() -> PollingWorkerConfig:
    return PollingWorkerConfig(
        worker_id=os.getenv("WORKER_ID", "polling-worker-1"),
        polling_interval_seconds=float(os.getenv("POLLING_INTERVAL_SECONDS", "1.0")),
        max_polling_interval_seconds=float(os.getenv("MAX_POLLING_INTERVAL_SECONDS", "10.0")),
        simulated_processing_seconds=float(os.getenv("SIMULATED_PROCESSING_SECONDS", "0.0")),
        stale_processing_reset_minutes=int(os.getenv("STALE_PROCESSING_RESET_MINUTES", "10")),
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    load_dotenv()
    configure_logging()

    engine = create_db_engine()
    repository = PollingPocRepository(engine)
    config = load_config()

    run_polling_worker(
        repository=repository,
        config=config,
    )


if __name__ == "__main__":
    main()
