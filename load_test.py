"""
Inserts matching batches into both polling_poc_raw_data and notify_poc_raw_data
at random intervals so both workers receive identical workloads.
"""

from __future__ import annotations

import logging
import os
import random
import time

from dotenv import load_dotenv
from sqlalchemy import text

from utilities import create_db_engine

INSERT_SQL = """
    INSERT INTO public.{table} (
        name,
        processing_timestamp,
        value,
        origin_time_stamp,
        data_source_id
    )
    SELECT
        'sensor_' || i,
        now(),
        random() * 100,
        now(),
        1
    FROM generate_series(1, :batch_size) AS i;
"""


def run_load_test(
    num_batches: int,
    min_batch_size: int,
    max_batch_size: int,
    min_interval_seconds: float,
    max_interval_seconds: float,
) -> None:
    engine = create_db_engine()

    polling_sql = text(INSERT_SQL.format(table="polling_poc_raw_data"))
    notify_sql = text(INSERT_SQL.format(table="notify_poc_raw_data"))

    logging.info(
        "Starting load test: %d batches, size=%d-%d, interval=%.1f-%.1fs",
        num_batches,
        min_batch_size,
        max_batch_size,
        min_interval_seconds,
        max_interval_seconds,
    )

    for i in range(1, num_batches + 1):
        batch_size = random.randint(min_batch_size, max_batch_size)

        with engine.begin() as conn:
            conn.execute(polling_sql, {"batch_size": batch_size})

        with engine.begin() as conn:
            conn.execute(notify_sql, {"batch_size": batch_size})

        logging.info("Batch %d/%d inserted (%d rows each table)", i, num_batches, batch_size)

        if i < num_batches:
            sleep_time = random.uniform(min_interval_seconds, max_interval_seconds)
            time.sleep(sleep_time)

    logging.info("Load test complete: %d batches inserted into both tables", num_batches)


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    run_load_test(
        num_batches=int(os.getenv("LOAD_TEST_NUM_BATCHES", "20")),
        min_batch_size=int(os.getenv("LOAD_TEST_MIN_BATCH_SIZE", "5")),
        max_batch_size=int(os.getenv("LOAD_TEST_MAX_BATCH_SIZE", "20")),
        min_interval_seconds=float(os.getenv("LOAD_TEST_MIN_INTERVAL", "2.0")),
        max_interval_seconds=float(os.getenv("LOAD_TEST_MAX_INTERVAL", "10.0")),
    )


if __name__ == "__main__":
    main()
