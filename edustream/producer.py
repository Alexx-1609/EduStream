# =============================================================================
# producer.py  —  LearningEventProducer
# PRD FR1: Simulate learning events
# PRD FR2: Ingest into Kafka topic
# NFR3: >= 100 events/minute
# LLD: produce_events(), connect_kafka()
# =============================================================================

import time
import random
from faker import Faker

import edustream.config as cfg
from edustream.models import LearningEvent, now_iso
from edustream.utils.logging_cfg import get_logger
from edustream.utils.retry import with_retry

log  = get_logger("producer")
fake = Faker()


def _make_student_pool(size: int) -> list[str]:
    return [f"STU-{str(i).zfill(5)}" for i in range(size)]


def _build_metadata(event_type: str) -> dict:
    if event_type == "quiz_attempt":
        return {"quiz_id": f"qz{random.randint(100,999)}", "score": random.randint(40,100), "max_score": 100, "duration_s": random.randint(120,1800)}
    elif event_type == "video_view":
        return {"video_id": f"VID-{random.randint(1,200)}", "watch_percent": round(random.uniform(0.1,1.0),2), "duration_s": random.randint(60,3600)}
    elif event_type == "assignment_submit":
        return {"assignment_id": f"ASGN-{random.randint(1,50)}", "late": random.choice([True,False]), "file_count": random.randint(1,5)}
    elif event_type == "resource_download":
        return {"resource_id": f"RES-{random.randint(1,500)}", "file_type": random.choice(["pdf","pptx","docx","csv"]), "size_kb": random.randint(50,5000)}
    elif event_type == "forum_post":
        return {"thread_id": f"THREAD-{random.randint(1,100)}", "word_count": random.randint(20,500)}
    else:
        return {"session_id": fake.uuid4()}


class LearningEventProducer:

    def __init__(self):
        self.student_pool = _make_student_pool(cfg.STUDENT_POOL_SIZE)
        self._kafka       = None
        self._sent_count  = 0
        self._error_count = 0

    @with_retry(max_retries=cfg.MAX_RETRIES, base_delay=cfg.RETRY_BASE_DELAY)
    def connect_kafka(self) -> None:
        from kafka import KafkaProducer
        log.info(f"Connecting to Kafka at {cfg.KAFKA_BROKER} ...")
        self._kafka = KafkaProducer(
            bootstrap_servers        = cfg.KAFKA_BROKER,
            value_serializer         = lambda v: v.encode("utf-8"),
            acks                     = 0,        # fire and forget — no timeout waiting for ack
            retries                  = 0,
            max_block_ms             = 5_000,
            request_timeout_ms       = 5_000,
            metadata_max_age_ms      = 5_000,
            connections_max_idle_ms  = 30_000,
        )
        log.info("Connected to Kafka")

    def _generate_event(self) -> LearningEvent:
        event_type = random.choice(cfg.EVENT_TYPES)
        user_id    = random.choice(self.student_pool)
        return LearningEvent(
            user_id       = user_id,
            event_type    = event_type,
            activity_type = event_type,
            timestamp     = now_iso(),
            metadata      = _build_metadata(event_type),
        )

    def _send(self, event: LearningEvent) -> None:
        """Fire and forget — no blocking wait."""
        self._kafka.send(cfg.KAFKA_TOPIC, value=event.to_json())

    def produce_events(self) -> None:
        self.connect_kafka()
        interval = 1.0 / cfg.EVENTS_PER_SECOND
        log.info(f"Producer running -> topic='{cfg.KAFKA_TOPIC}' rate={cfg.EVENTS_PER_SECOND}/sec (={cfg.EVENTS_PER_SECOND*60}/min)")
        last_log = time.time()

        while True:
            try:
                event = self._generate_event()
                self._send(event)
                self._sent_count += 1
                if time.time() - last_log >= 10:
                    log.info(f"Sent {self._sent_count} events | errors={self._error_count}")
                    last_log = time.time()
                time.sleep(interval)
            except KeyboardInterrupt:
                log.info(f"Producer stopped. Total sent: {self._sent_count}")
                if self._kafka:
                    self._kafka.flush()
                    self._kafka.close()
                break
            except Exception as e:
                self._error_count += 1
                log.error(f"Send failed: {e}")
                time.sleep(2)


if __name__ == "__main__":
    LearningEventProducer().produce_events()
