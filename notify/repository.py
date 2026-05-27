from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, text
from sqlalchemy.orm import sessionmaker


@dataclass(frozen=True)
class NotifyPocEvent:
    notify_poc_event_queue_id: int
    min_raw_data_id: int
    max_raw_data_id: int
    row_count: int


class NotifyPocRepository:
    def __init__(self, engine: Engine) -> None:
        self._session_factory = sessionmaker(
            bind=engine,
            expire_on_commit=False,
        )

    def claim_next_event(self, worker_id: str) -> NotifyPocEvent | None:
        sql = text("""
            WITH next_event AS (
                SELECT notify_poc_event_queue_id
                FROM public.notify_poc_event_queue
                WHERE status = 'unprocessed'
                ORDER BY created_at, notify_poc_event_queue_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE public.notify_poc_event_queue q
            SET
                status = 'processing',
                started_at = current_timestamp,
                worker_id = :worker_id
            FROM next_event
            WHERE q.notify_poc_event_queue_id =
                  next_event.notify_poc_event_queue_id
            RETURNING
                q.notify_poc_event_queue_id,
                q.min_raw_data_id,
                q.max_raw_data_id,
                q.row_count;
        """)

        with self._session_factory.begin() as session:
            row = session.execute(sql, {"worker_id": worker_id}).mappings().first()

        if row is None:
            return None

        return NotifyPocEvent(
            notify_poc_event_queue_id=row["notify_poc_event_queue_id"],
            min_raw_data_id=row["min_raw_data_id"],
            max_raw_data_id=row["max_raw_data_id"],
            row_count=row["row_count"],
        )

    def fetch_raw_rows_for_event(self, event: NotifyPocEvent) -> list[dict[str, Any]]:
        sql = text("""
            SELECT
                notify_poc_raw_data_id,
                name,
                processing_timestamp,
                value,
                created_time_stamp,
                origin_time_stamp,
                data_source_id
            FROM public.notify_poc_raw_data
            WHERE notify_poc_raw_data_id
                  BETWEEN :min_raw_data_id AND :max_raw_data_id
            ORDER BY notify_poc_raw_data_id;
        """)

        with self._session_factory() as session:
            rows = (
                session.execute(
                    sql,
                    {
                        "min_raw_data_id": event.min_raw_data_id,
                        "max_raw_data_id": event.max_raw_data_id,
                    },
                )
                .mappings()
                .all()
            )

        return [dict(row) for row in rows]

    def mark_event_processed(self, event_id: int) -> None:
        sql = text("""
            UPDATE public.notify_poc_event_queue
            SET
                status = 'processed',
                processed_at = current_timestamp
            WHERE notify_poc_event_queue_id = :event_id;
        """)

        with self._session_factory.begin() as session:
            session.execute(sql, {"event_id": event_id})

    def mark_event_failed(self, event_id: int, error_message: str) -> None:
        sql = text("""
            UPDATE public.notify_poc_event_queue
            SET
                status = 'failed',
                error_message = :error_message,
                processed_at = current_timestamp
            WHERE notify_poc_event_queue_id = :event_id;
        """)

        with self._session_factory.begin() as session:
            session.execute(
                sql,
                {
                    "event_id": event_id,
                    "error_message": error_message[:5000],
                },
            )

    def reset_stale_processing_events(self, stale_after_minutes: int = 10) -> int:
        sql = text("""
            UPDATE public.notify_poc_event_queue
            SET
                status = 'unprocessed',
                started_at = NULL,
                worker_id = NULL,
                error_message = NULL
            WHERE status = 'processing'
              AND started_at < current_timestamp - (:stale_after_minutes * interval '1 minute')
            RETURNING notify_poc_event_queue_id;
        """)

        with self._session_factory.begin() as session:
            rows = session.execute(
                sql,
                {"stale_after_minutes": stale_after_minutes},
            ).all()

        return len(rows)
