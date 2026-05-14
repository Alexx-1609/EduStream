# =============================================================================
# tests/test_consumer.py  —  Tests for the Consumer's processing logic
#
# Run:  python -m pytest tests/ -v
# =============================================================================

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from unittest.mock import MagicMock, patch
from edustream.models import LearningEvent, now_iso
from edustream.consumer import LearningEventConsumer, _anonymize


def make_event(**kwargs):
    defaults = dict(
        student_id="STU-00001",
        event_type="quiz_attempt",
        timestamp=now_iso(),
        metadata={"score": 85},
    )
    defaults.update(kwargs)
    return LearningEvent(**defaults)


# ── Anonymization tests ───────────────────────────────────────────────────────

def test_anonymize_returns_anon_prefix():
    result = _anonymize("STU-00001")
    assert result.startswith("ANON-")


def test_anonymize_is_deterministic():
    """Same input → same output (important for joining data later)."""
    assert _anonymize("STU-00001") == _anonymize("STU-00001")


def test_anonymize_different_ids():
    """Different students must get different hashes."""
    assert _anonymize("STU-00001") != _anonymize("STU-00002")


def test_anonymize_hides_original():
    """Original ID must not appear in the hash."""
    result = _anonymize("STU-00001")
    assert "STU-00001" not in result


# ── process_event tests ───────────────────────────────────────────────────────

def make_consumer_no_kafka():
    """Creates a consumer without actually connecting to Kafka or HDFS."""
    with patch("edustream.hdfs_writer.HDFSWriter.__init__", return_value=None):
        consumer = LearningEventConsumer.__new__(LearningEventConsumer)
        consumer._hdfs = MagicMock()
        consumer._processed = 0
        consumer._skipped   = 0
        consumer._last_log  = 0
        return consumer


def test_process_event_adds_processed_at():
    consumer = make_consumer_no_kafka()
    event    = make_event()
    result   = consumer.process_event(event)
    assert "processed_at" in result


def test_process_event_anonymizes_student_id():
    consumer = make_consumer_no_kafka()
    event    = make_event(student_id="STU-00001")
    result   = consumer.process_event(event)
    assert result["student_id"].startswith("ANON-")
    assert "STU-00001" not in result["student_id"]


def test_process_event_keeps_metadata():
    consumer = make_consumer_no_kafka()
    event    = make_event(metadata={"score": 95, "quiz_id": "QZ-001"})
    result   = consumer.process_event(event)
    assert result["metadata"]["score"] == 95


def test_process_event_adds_pipeline_version():
    consumer = make_consumer_no_kafka()
    result   = consumer.process_event(make_event())
    assert "pipeline_version" in result
