# =============================================================================
# config.py  —  All configuration for EduStream
# Matches PRD and LLD requirements exactly
# =============================================================================

# ── Kafka settings ────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "learning_events"
KAFKA_GROUP_ID = "edustream_group_v2"

# ── HDFS settings ─────────────────────────────────────────────────────────────
HDFS_HOST      = "http://localhost:9870"
HDFS_USER      = "root"
HDFS_BASE_PATH = "/data/learning_events"

# ── Producer settings ─────────────────────────────────────────────────────────
EVENTS_PER_SECOND   = 2           # 2/sec = 120/min → satisfies NFR3 (≥100/min)
STUDENT_POOL_SIZE   = 50
ANONYMIZATION_SALT  = "edustream_secret_2024"

# ── Consumer / writer settings ────────────────────────────────────────────────
BATCH_SIZE    = 50
BATCH_TIMEOUT = 30

# ── Retry settings ────────────────────────────────────────────────────────────
MAX_RETRIES      = 5
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY  = 30.0

# ── Event types — matches PRD FR1 ─────────────────────────────────────────────
# PRD FR1: "login, quiz, video view"
EVENT_TYPES = [
    "login",
    "logout",
    "video_view",
    "quiz_attempt",
    "assignment_submit",
    "resource_download",
    "forum_post",
]

# ── Local fallback ────────────────────────────────────────────────────────────
LOCAL_OUTPUT_DIR = "./data/hdfs_fallback"
