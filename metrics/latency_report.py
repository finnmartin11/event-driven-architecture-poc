from __future__ import annotations

import logging

from sqlalchemy import Engine, text
from sqlalchemy.orm import sessionmaker


class LatencyReport:
    """Queries event queue tables and computes latency percentiles."""

    def __init__(self, engine: Engine) -> None:
        self._session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    def _run_report(self, strategy: str, table: str, id_col: str) -> dict | None:
        sql = text(f"""
            SELECT
                count(*)                                                       AS total_events,
                count(*) FILTER (WHERE status = 'processed')                   AS processed_events,
                count(*) FILTER (WHERE status = 'failed')                      AS failed_events,
                count(*) FILTER (WHERE status = 'unprocessed')                 AS unprocessed_events,

                -- pickup latency: created_at -> started_at
                round(extract(epoch FROM avg(started_at - created_at))    * 1000, 2) AS pickup_avg_ms,
                round(extract(epoch FROM min(started_at - created_at))    * 1000, 2) AS pickup_min_ms,
                round(extract(epoch FROM max(started_at - created_at))    * 1000, 2) AS pickup_max_ms,
                round(extract(epoch FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY started_at - created_at)) * 1000, 2) AS pickup_p50_ms,
                round(extract(epoch FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY started_at - created_at)) * 1000, 2) AS pickup_p95_ms,
                round(extract(epoch FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY started_at - created_at)) * 1000, 2) AS pickup_p99_ms,

                -- processing duration: started_at -> processed_at
                round(extract(epoch FROM avg(processed_at - started_at))    * 1000, 2) AS processing_avg_ms,
                round(extract(epoch FROM min(processed_at - started_at))    * 1000, 2) AS processing_min_ms,
                round(extract(epoch FROM max(processed_at - started_at))    * 1000, 2) AS processing_max_ms,
                round(extract(epoch FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY processed_at - started_at)) * 1000, 2) AS processing_p50_ms,
                round(extract(epoch FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY processed_at - started_at)) * 1000, 2) AS processing_p95_ms,

                -- end-to-end: created_at -> processed_at
                round(extract(epoch FROM avg(processed_at - created_at))    * 1000, 2) AS e2e_avg_ms,
                round(extract(epoch FROM min(processed_at - created_at))    * 1000, 2) AS e2e_min_ms,
                round(extract(epoch FROM max(processed_at - created_at))    * 1000, 2) AS e2e_max_ms,
                round(extract(epoch FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY processed_at - created_at)) * 1000, 2) AS e2e_p50_ms,
                round(extract(epoch FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY processed_at - created_at)) * 1000, 2) AS e2e_p95_ms,
                round(extract(epoch FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY processed_at - created_at)) * 1000, 2) AS e2e_p99_ms,

                -- throughput
                round(sum(row_count)::numeric / NULLIF(extract(epoch FROM max(processed_at) - min(created_at)), 0), 2) AS rows_per_second

            FROM public.{table}
            WHERE status IN ('processed', 'failed');
        """)

        with self._session_factory() as session:
            row = session.execute(sql).mappings().first()

        if row is None or row["total_events"] == 0:
            logging.warning("No processed events found for %s", strategy)
            return None

        return {"strategy": strategy, **dict(row)}

    def print_report(self, strategies: list[str] | None = None) -> None:
        reports = {
            "polling": ("polling_poc_event_queue", "polling_poc_event_queue_id"),
            "notify": ("notify_poc_event_queue", "notify_poc_event_queue_id"),
        }

        if strategies:
            reports = {k: v for k, v in reports.items() if k in strategies}

        results = []
        for strategy, (table, id_col) in reports.items():
            result = self._run_report(strategy, table, id_col)
            if result:
                results.append(result)

        if not results:
            print("No data to report.")
            return

        for r in results:
            print(f"\n{'=' * 60}")
            print(f"  Strategy: {r['strategy'].upper()}")
            print(f"{'=' * 60}")
            print(f"  Events: {r['total_events']} total, {r['processed_events']} processed, {r['failed_events']} failed, {r['unprocessed_events']} unprocessed")
            print(f"  Throughput: {r['rows_per_second']} rows/sec")
            print()
            print(f"  Pickup Latency (created_at -> started_at):")
            print(f"    avg={r['pickup_avg_ms']}ms  min={r['pickup_min_ms']}ms  max={r['pickup_max_ms']}ms")
            print(f"    p50={r['pickup_p50_ms']}ms  p95={r['pickup_p95_ms']}ms  p99={r['pickup_p99_ms']}ms")
            print()
            print(f"  Processing Duration (started_at -> processed_at):")
            print(f"    avg={r['processing_avg_ms']}ms  min={r['processing_min_ms']}ms  max={r['processing_max_ms']}ms")
            print(f"    p50={r['processing_p50_ms']}ms  p95={r['processing_p95_ms']}ms")
            print()
            print(f"  End-to-End (created_at -> processed_at):")
            print(f"    avg={r['e2e_avg_ms']}ms  min={r['e2e_min_ms']}ms  max={r['e2e_max_ms']}ms")
            print(f"    p50={r['e2e_p50_ms']}ms  p95={r['e2e_p95_ms']}ms  p99={r['e2e_p99_ms']}ms")

        if len(results) == 2:
            p, n = results[0], results[1]
            if p["strategy"] != "polling":
                p, n = n, p
            print(f"\n{'=' * 60}")
            print(f"  COMPARISON (polling vs notify)")
            print(f"{'=' * 60}")
            if p["pickup_avg_ms"] and n["pickup_avg_ms"]:
                speedup = float(p["pickup_avg_ms"]) / float(n["pickup_avg_ms"])
                print(f"  Pickup latency avg: polling={p['pickup_avg_ms']}ms  notify={n['pickup_avg_ms']}ms  ({speedup:.1f}x faster)")
            if p["e2e_avg_ms"] and n["e2e_avg_ms"]:
                speedup = float(p["e2e_avg_ms"]) / float(n["e2e_avg_ms"])
                print(f"  E2E latency avg:    polling={p['e2e_avg_ms']}ms  notify={n['e2e_avg_ms']}ms  ({speedup:.1f}x faster)")
