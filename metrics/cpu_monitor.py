from __future__ import annotations

import csv
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil


@dataclass
class CpuSample:
    timestamp: float
    cpu_percent: float
    process_cpu_percent: float
    memory_rss_mb: float


class CpuMonitor:
    """Samples CPU and memory usage in a background thread."""

    def __init__(self, interval_seconds: float = 1.0) -> None:
        self._interval = interval_seconds
        self._samples: list[CpuSample] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = psutil.Process(os.getpid())

    def _sample_loop(self) -> None:
        # Prime the cpu_percent call (first call always returns 0)
        self._process.cpu_percent()
        psutil.cpu_percent()

        while not self._stop_event.is_set():
            sample = CpuSample(
                timestamp=time.time(),
                cpu_percent=psutil.cpu_percent(),
                process_cpu_percent=self._process.cpu_percent(),
                memory_rss_mb=self._process.memory_info().rss / (1024 * 1024),
            )
            self._samples.append(sample)
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._samples.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()
        logging.info("CPU monitor started (interval=%.1fs)", self._interval)

    def stop(self) -> None:
        if self._thread is None:
            return

        self._stop_event.set()
        self._thread.join(timeout=5)
        self._thread = None
        logging.info("CPU monitor stopped (%d samples collected)", len(self._samples))

    @property
    def samples(self) -> list[CpuSample]:
        return list(self._samples)

    def write_csv(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "cpu_percent", "process_cpu_percent", "memory_rss_mb"])
            for s in self._samples:
                writer.writerow([s.timestamp, s.cpu_percent, s.process_cpu_percent, s.memory_rss_mb])

        logging.info("CPU metrics written to %s", path)

    def print_summary(self) -> None:
        if not self._samples:
            print("No CPU samples collected.")
            return

        cpu_vals = [s.cpu_percent for s in self._samples]
        proc_vals = [s.process_cpu_percent for s in self._samples]
        mem_vals = [s.memory_rss_mb for s in self._samples]

        print(f"\n{'=' * 60}")
        print(f"  CPU / Memory Summary ({len(self._samples)} samples)")
        print(f"{'=' * 60}")
        print(f"  System CPU:  avg={sum(cpu_vals)/len(cpu_vals):.1f}%  max={max(cpu_vals):.1f}%")
        print(f"  Process CPU: avg={sum(proc_vals)/len(proc_vals):.1f}%  max={max(proc_vals):.1f}%")
        print(f"  Process RSS: avg={sum(mem_vals)/len(mem_vals):.1f}MB  max={max(mem_vals):.1f}MB")
