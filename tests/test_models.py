# =============================================================================
# tests/test_models.py  —  Tests for LearningEvent
#
# Run:  python -m pytest tests/ -v
# =============================================================================

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pytest
from edustream.models import LearningEvent, now_iso


# ── Helpers ───────────────────────────────────────────────────────────────────
def make_event(**overrides) -> LearningEvent:
    """Creates a valid event with optional field overrides."""
    defaults = dict(
        student_id="STU-00001",
        event_type="quiz_attempt",
        timestamp=now_iso(),
        metadata={"score": 85},
    )
    defaults.update(overrides)
    return LearningEvent(**defaults)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_valid_event_passes():
    event = make_event()
    valid, reason = event.is_valid()
    assert valid, f"Expected valid but got: {reason}"


def test_event_id_auto_generated():
    e1 = make_event()
    e2 = make_event()
    assert e1.event_id != e2.event_id, "Each event must have a unique ID"


def test_json_roundtrip():
    """Serializing and deserializing should produce the same event."""
    original = make_event()
    json_str  = original.to_json()
    recovered = LearningEvent.from_json(json_str)
    assert recovered.event_id   == original.event_id
    assert recovered.student_id == original.student_id
    assert recovered.event_type == original.event_type
    assert recovered.timestamp  == original.timestamp
    assert recovered.metadata   == original.metadata


def test_to_json_is_valid_json():
    event = make_event()
    parsed = json.loads(event.to_json())   # should not raise
    assert "event_id" in parsed
    assert "student_id" in parsed


def test_missing_student_id_is_invalid():
    event = make_event(student_id="")
    valid, reason = event.is_valid()
    assert not valid
    assert "student_id" in reason.lower()


def test_missing_event_type_is_invalid():
    event = make_event(event_type="")
    valid, reason = event.is_valid()
    assert not valid


def test_bad_timestamp_is_invalid():
    event = make_event(timestamp="not-a-date")
    valid, reason = event.is_valid()
    assert not valid
    assert "timestamp" in reason.lower()


def test_metadata_must_be_dict():
    event = make_event(metadata="not a dict")   # type: ignore
    valid, reason = event.is_valid()
    assert not valid
    assert "metadata" in reason.lower()


def test_all_event_types():
    """Every event type in the config should produce a valid event."""
    import edustream.config as cfg
    for etype in cfg.EVENT_TYPES:
        event = make_event(event_type=etype)
        valid, reason = event.is_valid()
        assert valid, f"Event type '{etype}' failed validation: {reason}"
