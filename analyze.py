# =============================================================================
# analyze.py  —  Reads stored events and prints a summary report
#
# Run:  python analyze.py
# =============================================================================

import json
import os
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent))
import edustream.config as cfg


def load_events(base_dir: str) -> list[dict]:
    """Reads all .json files from the storage folder."""
    events = []
    base = Path(base_dir)
    if not base.exists():
        print(f"No data folder found at: {base_dir}")
        print("Run the pipeline first to generate events.")
        return []

    for filepath in sorted(base.rglob("*.json")):
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return events


def print_report(events: list[dict]) -> None:
    if not events:
        print("No events to analyze.")
        return

    print("\n" + "═" * 60)
    print("  EduStream  —  Pipeline Analysis Report")
    print("═" * 60)

    # ── Total events ─────────────────────────────────────────────
    print(f"\n  Total events processed : {len(events)}")

    # ── Events by type ───────────────────────────────────────────
    type_counts = Counter(e["event_type"] for e in events)
    print(f"\n  Events by type:")
    for etype, count in type_counts.most_common():
        bar = "█" * (count * 20 // len(events))
        print(f"    {etype:<22}  {count:>5}  {bar}")

    # ── Unique (anonymized) students ──────────────────────────────
    unique_students = len(set(e["student_id"] for e in events))
    print(f"\n  Unique students (anonymized) : {unique_students}")

    # ── Most active students ──────────────────────────────────────
    student_counts = Counter(e["student_id"] for e in events)
    print(f"\n  Top 5 most active students:")
    for sid, count in student_counts.most_common(5):
        print(f"    {sid}  →  {count} events")

    # ── Quiz stats ────────────────────────────────────────────────
    quiz_scores = [
        e["metadata"]["score"]
        for e in events
        if e["event_type"] == "quiz_attempt" and "score" in e.get("metadata", {})
    ]
    if quiz_scores:
        avg = sum(quiz_scores) / len(quiz_scores)
        print(f"\n  Quiz attempts : {len(quiz_scores)}")
        print(f"  Average score : {avg:.1f} / 100")
        print(f"  Highest score : {max(quiz_scores)}")
        print(f"  Lowest score  : {min(quiz_scores)}")

    # ── Time range ────────────────────────────────────────────────
    timestamps = [e["timestamp"] for e in events if "timestamp" in e]
    if timestamps:
        print(f"\n  Earliest event : {min(timestamps)[:19]}")
        print(f"  Latest event   : {max(timestamps)[:19]}")

    # ── Pipeline version ──────────────────────────────────────────
    versions = Counter(e.get("pipeline_version", "?") for e in events)
    print(f"\n  Pipeline versions seen : {dict(versions)}")

    print("\n" + "═" * 60 + "\n")


if __name__ == "__main__":
    print("Loading events from storage …")
    events = load_events(cfg.LOCAL_OUTPUT_DIR)
    print_report(events)
