from __future__ import annotations

import logging
import select

import psycopg
from psycopg import sql


class PostgresNotificationListener:
    def __init__(self, connection_string: str, channel: str) -> None:
        self._connection_string = connection_string
        self._channel = channel
        self._conn: psycopg.Connection | None = None

    def connect(self) -> None:
        self.close()

        self._conn = psycopg.connect(
            self._connection_string,
            autocommit=True,
        )
        
        with self._conn.cursor() as cur:
            cur.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self._channel)))

        logging.info("Listening on Postgres channel: %s", self._channel)

    def wait_for_notification(self, timeout_seconds: float) -> bool:
        if self._conn is None or self._conn.closed:
            self.connect()

        assert self._conn is not None

        ready, _, _ = select.select([self._conn], [], [], timeout_seconds)

        if not ready:
            logging.info("Notification wait timed out after %.3fs", timeout_seconds)
            return False

        received_any = False

        for notification in self._conn.notifies(timeout=0):
            received_any = True
            logging.info(
                "Received notification channel=%s payload=%s",
                notification.channel,
                notification.payload,
            )

        return received_any

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
