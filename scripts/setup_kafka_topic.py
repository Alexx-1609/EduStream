# =============================================================================
# scripts/setup_kafka_topic.py
# Run ONCE after Docker starts to create the Kafka topic.
# Usage:  python scripts/setup_kafka_topic.py
# =============================================================================

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import edustream.config as cfg
from edustream.utils.logging_cfg import get_logger

log = get_logger("setup")


def create_topic():
    log.info(f"Connecting to Kafka at {cfg.KAFKA_BROKER} ...")
    admin = KafkaAdminClient(bootstrap_servers=cfg.KAFKA_BROKER)
    topic = NewTopic(
        name=cfg.KAFKA_TOPIC,
        num_partitions=3,
        replication_factor=1,
    )
    try:
        admin.create_topics([topic])
        log.info(f"Topic '{cfg.KAFKA_TOPIC}' created with 3 partitions")
    except TopicAlreadyExistsError:
        log.info(f"Topic '{cfg.KAFKA_TOPIC}' already exists — nothing to do")
    finally:
        admin.close()


if __name__ == "__main__":
    create_topic()
