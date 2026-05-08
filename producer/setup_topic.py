"""
producer/setup_topic.py
========================
Creates the Kafka topic before streaming begins.
Run this once before starting the producer.
"""

import os
import sys
import time
import logging
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("setup_topic")

KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "reviews-topic")
NUM_PARTITIONS   = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
REPLICATION      = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
MAX_RETRIES      = 10
RETRY_DELAY      = 5


def wait_for_kafka(broker: str, retries: int = MAX_RETRIES) -> KafkaAdminClient:
    """Wait for Kafka to be ready and return admin client."""
    for attempt in range(1, retries + 1):
        try:
            client = KafkaAdminClient(
                bootstrap_servers=[broker],
                client_id="topic-setup",
                request_timeout_ms=10000,
            )
            logger.info(f"✅ Connected to Kafka at {broker}")
            return client
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}). Waiting {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    raise RuntimeError(f"Kafka unreachable at {broker} after {retries} attempts")


def create_topic(admin_client: KafkaAdminClient, topic: str, partitions: int, replication: int) -> None:
    """Create Kafka topic if it doesn't already exist."""
    new_topic = NewTopic(
        name=topic,
        num_partitions=partitions,
        replication_factor=replication,
    )
    try:
        admin_client.create_topics([new_topic])
        logger.info(f"✅ Topic '{topic}' created ({partitions} partitions, {replication} replicas)")
    except TopicAlreadyExistsError:
        logger.info(f"ℹ️  Topic '{topic}' already exists — skipping creation")
    except Exception as e:
        logger.error(f"Error creating topic: {e}")
        raise


def main() -> None:
    logger.info(f"Setting up Kafka topic: {KAFKA_TOPIC}")
    admin = wait_for_kafka(KAFKA_BROKER)
    create_topic(admin, KAFKA_TOPIC, NUM_PARTITIONS, REPLICATION)

    # List existing topics
    topics = admin.list_topics()
    logger.info(f"Existing topics: {topics}")
    admin.close()


if __name__ == "__main__":
    main()
