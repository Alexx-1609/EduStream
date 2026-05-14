# =============================================================================
# consumer.py  —  LearningEventConsumer
# PRD FR3: Consume events from Kafka
# PRD FR4: Process/transform (anonymize, add timestamp)
# PRD FR5: Store in HDFS
# PRD FR6: Monitoring/logging
# LLD: consume_events(), process_event()
# =============================================================================

import time
import hashlib
import json

import edustream.config as cfg
from edustream.models import LearningEvent, now_iso
from edustream.hdfs_writer import HDFSWriter
from edustream.utils.logging_cfg import get_logger
from edustream.utils.retry import with_retry

log = get_logger("consumer")


def _anonymize(user_id: str) -> str:
    """PRD FR4: Anonymize user_id with SHA-256 hash."""
    raw = (cfg.ANONYMIZATION_SALT + user_id).encode("utf-8")
    return "ANON-" + hashlib.sha256(raw).hexdigest()[:16].upper()


class LearningEventConsumer:

    def __init__(self):
        self._kafka     = None
        self._hdfs      = HDFSWriter()
        self._processed = 0
        self._skipped   = 0
        self._last_log  = time.time()

    @with_retry(max_retries=cfg.MAX_RETRIES, base_delay=cfg.RETRY_BASE_DELAY)
    def connect_kafka(self) -> None:
        from kafka import KafkaConsumer
        log.info(f"Connecting to Kafka at {cfg.KAFKA_BROKER} ...")
        self._kafka = KafkaConsumer(
            cfg.KAFKA_TOPIC,
            bootstrap_servers     = [cfg.KAFKA_BROKER],
            group_id              = cfg.KAFKA_GROUP_ID,
            auto_offset_reset     = "earliest",
            enable_auto_commit    = True,
            value_deserializer    = lambda m: m.decode("utf-8"),
            session_timeout_ms    = 10_000,
            heartbeat_interval_ms = 3_000,
            request_timeout_ms    = 15_000,
            fetch_max_wait_ms     = 500,
            fetch_min_bytes       = 1,
        )
        log.info(f"Subscribed to topic: {cfg.KAFKA_TOPIC}")

    def process_event(self, event: LearningEvent) -> dict:
        """PRD FR4: Transform — anonymize + add processing timestamp."""
        return {
            "user_id":          _anonymize(event.user_id),
            "event_type":       event.event_type,
            "timestamp":        event.timestamp,
            "metadata":         event.metadata,
            "event_id":         event.event_id,
            "student_id":       _anonymize(event.student_id),
            "activity_type":    event.activity_type,
            "processed_at":     now_iso(),
            "pipeline_version": "1.0",
        }

    def consume_events(self) -> None:
        self.connect_kafka()
        log.info("Consumer running — waiting for events ...")

        try:
            while True:
                msg_pack = self._kafka.poll(timeout_ms=1000, max_records=100)

                for tp, messages in msg_pack.items():
                    for message in messages:
                        raw_json = message.value
                        try:
                            event = LearningEvent.from_json(raw_json)
                        except (json.JSONDecodeError, TypeError) as e:
                            log.warning(f"Malformed event skipped: {e}")
                            self._skipped += 1
                            continue

                        valid, reason = event.is_valid()
                        if not valid:
                            log.warning(f"Invalid event skipped: {reason}")
                            self._skipped += 1
                            continue

                        processed = self.process_event(event)
                        self._hdfs.write_to_hdfs(processed)
                        self._processed += 1

                if time.time() - self._last_log >= 10:
                    log.info(
                        f"[FR6 Monitoring] processed={self._processed} | "
                        f"skipped={self._skipped} | "
                        f"written_to_hdfs={self._hdfs._written_count}"
                    )
                    self._last_log = time.time()

        except KeyboardInterrupt:
            log.info("Consumer stopped.")
            self._hdfs.flush()
            log.info(
                f"Final stats -> processed={self._processed} | "
                f"skipped={self._skipped} | "
                f"written={self._hdfs._written_count}"
            )
        finally:
            if self._kafka:
                self._kafka.close()


if __name__ == "__main__":
    LearningEventConsumer().consume_events()
