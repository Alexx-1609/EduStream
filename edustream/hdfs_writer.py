# =============================================================================
# hdfs_writer.py  —  Stable Local Fallback Version
# =============================================================================

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import edustream.config as cfg
from edustream.utils.logging_cfg import get_logger

log = get_logger("hdfs_writer")


class HDFSWriter:

    def __init__(self):

        self._client = None

        self._use_hdfs = False

        self._batch = []

        self._last_flush = time.time()

        self._written_count = 0

        self._connect()

    # ─────────────────────────────────────────────────────────────
    # Local fallback setup
    # ─────────────────────────────────────────────────────────────
    def _connect(self):

        Path(cfg.LOCAL_OUTPUT_DIR).mkdir(
            parents=True,
            exist_ok=True
        )

        log.info(
            f"✓ Local fallback storage enabled at: "
            f"{cfg.LOCAL_OUTPUT_DIR}"
        )

    # ─────────────────────────────────────────────────────────────
    # Add event to batch
    # ─────────────────────────────────────────────────────────────
    def add(self, event_dict):

        self._batch.append(event_dict)

        batch_full = len(self._batch) >= cfg.BATCH_SIZE

        timeout_passed = (
            time.time() - self._last_flush
        ) >= cfg.BATCH_TIMEOUT

        if batch_full or timeout_passed:
            self.flush()

    # ─────────────────────────────────────────────────────────────
    # Flush batch
    # ─────────────────────────────────────────────────────────────
    def flush(self):

        if not self._batch:
            return

        batch_to_write = self._batch.copy()

        self._batch.clear()

        self._last_flush = time.time()

        path = self._build_path()

        local_path = str(
            Path(cfg.LOCAL_OUTPUT_DIR) /
            Path(path).relative_to(cfg.HDFS_BASE_PATH)
        )

        content = "\n".join(
            json.dumps(e)
            for e in batch_to_write
        )

        self._write_local(local_path, content)

        self._written_count += len(batch_to_write)

        log.info(
            f"💾 Wrote {len(batch_to_write)} events → "
            f"{local_path} "
            f"(total written: {self._written_count})"
        )

    # ─────────────────────────────────────────────────────────────
    # Build output path
    # ─────────────────────────────────────────────────────────────
    def _build_path(self):

        now = datetime.now(timezone.utc)

        date_part = (
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}"
        )

        filename = (
            f"events_{now.strftime('%H%M%S_%f')}.json"
        )

        return (
            f"{cfg.HDFS_BASE_PATH}/"
            f"{date_part}/"
            f"{filename}"
        )

    # ─────────────────────────────────────────────────────────────
    # Local file write
    # ─────────────────────────────────────────────────────────────
    def _write_local(self, path, content):

        Path(path).parent.mkdir(
            parents=True,
            exist_ok=True
        )

        Path(path).write_text(
            content,
            encoding="utf-8"
        )

    # ─────────────────────────────────────────────────────────────
    # Public aliases
    # ─────────────────────────────────────────────────────────────
    def connect_hdfs(self):
        self._connect()

    def write_to_hdfs(self, event_dict):
        self.add(event_dict)