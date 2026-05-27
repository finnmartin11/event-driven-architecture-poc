from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from traceback import format_exc
from typing import Any

from dotenv import load_dotenv

from notify.listener import PostgresNotificationListener
from notify.repository import NotifyPocEvent, NotifyPocRepository
from utilities import create_db_engine, get_key_vault_secret

NOTIFY_CHANNEL = "notify_poc_event_queue_inserted"


@dataclass(frozen=True)
class NotifyWorkerConfig:
    worker_id: str
    notification_wait_timeout_seconds: float
    simulated_processing_seconds: float
    stale_processing_reset_minutes: int


def get_listener_connection_string() -> str:
    db_url_secret_name = os.getenv("DATABASE_URL")

    if db_url_secret_name is None:
        raise ValueError("DATABASE_URL env var cannot be None")

    connection_string = get_key_vault_secret(db_url_secret_name)

    connection_string = connection_string.replace(
        "postgresql+psycopg2://",
        "postgresql://",
        1,
    ).replace(
        "postgresql+psycopg://",
        "postgresql://",
        1,
    )

    separator = "&" if "?" in connection_string else "?"
    return f"{connection_string}{separator}application_name=notify_poc_listener"


def process_rows(rows: list[dict[str, Any]], simulated_processing_seconds: float) -> None:
    if simulated_processing_seconds > 0:
        time.sleep(simulated_processing_seconds)

    logging.info("Processed %s raw rows", len(rows))


def process_event(
    repository: NotifyPocRepository,
    event: NotifyPocEvent,
    config: NotifyWorkerConfig,
) -> None:
    event_id = event.notify_poc_event_queue_id

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


def drain_available_events(
    repository: NotifyPocRepository,
    config: NotifyWorkerConfig,
) -> int:
    processed_count = 0

    while True:
        event = repository.claim_next_event(config.worker_id)

        if event is None:
            break

        process_event(
            repository=repository,
            event=event,
            config=config,
        )

        processed_count += 1

    return processed_count


def run_notify_worker(
    repository: NotifyPocRepository,
    listener: PostgresNotificationListener,
    config: NotifyWorkerConfig,
) -> None:
    logging.info("Starting notify worker: %s", config.worker_id)

    reset_count = repository.reset_stale_processing_events(
        stale_after_minutes=config.stale_processing_reset_minutes,
    )
    logging.info("Reset %s stale processing events", reset_count)

    listener.connect()

    while True:
        try:
            processed_count = drain_available_events(repository, config)

            if processed_count > 0:
                logging.info("Drained %s available events", processed_count)
                continue

            listener.wait_for_notification(
                timeout_seconds=config.notification_wait_timeout_seconds,
            )

        except KeyboardInterrupt:
            logging.info("Stopping notify worker")
            listener.close()
            return

        except Exception:
            logging.exception("Unexpected notify worker error")

            try:
                listener.connect()
            except Exception:
                logging.exception("Failed reconnecting notification listener")

            time.sleep(1)


def load_config() -> NotifyWorkerConfig:
    return NotifyWorkerConfig(
        worker_id=os.getenv("NOTIFY_WORKER_ID", "notify-worker-1"),
        notification_wait_timeout_seconds=float(os.getenv("NOTIFICATION_WAIT_TIMEOUT_SECONDS", "30.0")),
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
    repository = NotifyPocRepository(engine)

    listener = PostgresNotificationListener(
        connection_string=get_listener_connection_string(),
        channel=NOTIFY_CHANNEL,
    )

    config = load_config()

    from metrics.cpu_monitor import CpuMonitor

    cpu_monitor = CpuMonitor()
    cpu_monitor.start()

    try:
        run_notify_worker(
            repository=repository,
            listener=listener,
            config=config,
        )
    finally:
        cpu_monitor.stop()
        cpu_monitor.print_summary()
        cpu_monitor.write_csv("output/notify_cpu.csv")


if __name__ == "__main__":
    main()
