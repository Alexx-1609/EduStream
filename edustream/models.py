# =============================================================================
# models.py  —  LearningEvent data model
#
# Field names match BOTH PRD and LLD:
#   PRD says:  user_id, event_type, timestamp, metadata
#   LLD says:  event_id, student_id, timestamp, activity_type, metadata
#
# We include ALL fields from both documents.
# =============================================================================

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone


@dataclass
class LearningEvent:
    # PRD fields: user_id, event_type, timestamp, metadata
    # LLD fields: event_id, student_id, activity_type, timestamp, metadata
    user_id:       str    # PRD: user_id (same as student_id in LLD)
    event_type:    str    # PRD: event_type
    activity_type: str    # LLD: activity_type (same value as event_type)
    timestamp:     str    # Both
    metadata:      dict   # Both
    event_id:      str    = field(default_factory=lambda: str(uuid.uuid4()))  # LLD
    student_id:    str    = ""   # LLD: student_id (same as user_id, set in __post_init__)

    def __post_init__(self):
        # Keep student_id and user_id in sync (LLD uses student_id, PRD uses user_id)
        if not self.student_id:
            self.student_id = self.user_id

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> "LearningEvent":
        data = json.loads(json_str)
        return cls(**data)

    def is_valid(self) -> tuple[bool, str]:
        if not self.user_id or not isinstance(self.user_id, str):
            return False, "Missing or invalid user_id"
        if not self.event_type or not isinstance(self.event_type, str):
            return False, "Missing or invalid event_type"
        if not self.timestamp:
            return False, "Missing timestamp"
        if not isinstance(self.metadata, dict):
            return False, "metadata must be a dict"
        try:
            datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False, f"Invalid timestamp format: {self.timestamp}"
        return True, ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
