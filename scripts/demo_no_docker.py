# =============================================================================
# scripts/demo_no_docker.py
#
# Runs the FULL pipeline in a single Python process WITHOUT Docker or Kafka.
# Works on Python 3.12, 3.13, 3.14.
#
# HOW TO RUN (from ANY folder — it auto-detects its location):
#   python scripts/demo_no_docker.py        (from edustream folder)
#   python demo_no_docker.py               (from scripts folder)
# =============================================================================

import sys
import os
from pathlib import Path

# ── Auto-detect project root regardless of where you run this from ────────────
THIS_FILE   = Path(__file__).resolve()
SCRIPTS_DIR = THIS_FILE.parent          # .../edustream/scripts/
PROJECT_ROOT = SCRIPTS_DIR.parent       # .../edustream/

# Add project root to Python path so "import edustream" always works
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Also change working directory to project root so relative paths work
os.chdir(PROJECT_ROOT)

# ── Now safe to import project modules ────────────────────────────────────────
import json
import random
import time
from unittest.mock import MagicMock

import edustream.config as cfg
from edustream.models import LearningEvent, now_iso
from edustream.producer import _build_metadata, _make_student_pool
from edustream.consumer import LearningEventConsumer, _anonymize
from edustream.hdfs_writer import HDFSWriter
from edustream.utils.logging_cfg import get_logger

log = get_logger("demo")


def run_demo(num_events: int = 200):
    log.info("=" * 55)
    log.info("  EduStream  --  Local Demo (no Docker required)")
    log.info("=" * 55)
    log.info(f"  Running from: {PROJECT_ROOT}")
    log.info(f"  Output will be saved to: {PROJECT_ROOT / cfg.LOCAL_OUTPUT_DIR}")
    log.info("=" * 55)

    student_pool = _make_student_pool(cfg.STUDENT_POOL_SIZE)
    writer       = HDFSWriter()   # auto-falls back to local storage

    # Build a consumer without needing Kafka
    consumer = LearningEventConsumer.__new__(LearningEventConsumer)
    consumer._hdfs        = writer
    consumer._processed   = 0
    consumer._skipped     = 0
    consumer._last_log    = time.time()

    log.info(f"Generating {num_events} events ...")
    start = time.time()

    for i in range(num_events):
        # 1. Generate a random event
        event_type = random.choice(cfg.EVENT_TYPES)
        event = LearningEvent(
            user_id = random.choice(student_pool),
            activity_type = event_type,
            event_type = event_type,
            timestamp  = now_iso(),
            metadata   = _build_metadata(event_type),
        )

        # 2. Every 40 events inject a deliberately bad event (tests error handling)
        if i > 0 and i % 40 == 0:
            event.student_id = ""
            log.info(f"  [event {i}] Injecting a bad event to test error handling ...")

        # 3. Validate — skip bad ones, never crash
        valid, reason = event.is_valid()
        if not valid:
            log.warning(f"  Bad event skipped: {reason}")
            consumer._skipped += 1
            continue

        # 4. Process and write
        processed = consumer.process_event(event)
        writer.write_to_hdfs(processed)
        consumer._processed += 1

        time.sleep(0.01)

    # Force-write any remaining events in the buffer
    writer.flush()

    elapsed = time.time() - start
    log.info("-" * 55)
    log.info(f"  Done in {elapsed:.1f}s")
    log.info(f"  Events generated : {num_events}")
    log.info(f"  Events processed : {consumer._processed}")
    log.info(f"  Events skipped   : {consumer._skipped}")
    log.info(f"  Files written    : {writer._written_count}")
    log.info(f"  Saved to         : {PROJECT_ROOT / cfg.LOCAL_OUTPUT_DIR}")
    log.info("-" * 55)

    # Run the analysis report
    log.info("\nRunning analysis report ...\n")

    # Import analyze from project root
    analyze_path = PROJECT_ROOT / "analyze.py"
    import importlib.util
    spec = importlib.util.spec_from_file_location("analyze", analyze_path)
    analyze = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(analyze)

    events = analyze.load_events(cfg.LOCAL_OUTPUT_DIR)
    analyze.print_report(events)


if __name__ == "__main__":
    run_demo(num_events=200)
