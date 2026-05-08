"""
producer/kafka_producer.py
===========================
Real-time Amazon review streaming producer.

Reads reviews from CSV and publishes them to the Kafka 'reviews-topic'
at a configurable rate, simulating real-time ingestion.
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/producer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",    "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",      "reviews-topic")
CSV_PATH       = os.getenv("CSV_PATH",         "data/Reviews.csv")
PRODUCER_DELAY = float(os.getenv("PRODUCER_DELAY", "1"))
MAX_RETRIES    = int(os.getenv("KAFKA_MAX_RETRIES", "15"))
RETRY_DELAY    = int(os.getenv("KAFKA_RETRY_DELAY", "5"))


class AmazonReviewProducer:
    """Streams Amazon reviews from CSV → Kafka topic."""

    def __init__(self, broker=KAFKA_BROKER, topic=KAFKA_TOPIC,
                 delay=PRODUCER_DELAY, csv_path=CSV_PATH):
        self.broker   = broker
        self.topic    = topic
        self.delay    = delay
        self.csv_path = csv_path
        self.producer: Optional[KafkaProducer] = None
        self._running = True

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def connect(self) -> None:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Connecting to Kafka at {self.broker} (attempt {attempt}/{MAX_RETRIES})")
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.broker],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=3,
                    retry_backoff_ms=1000,
                    request_timeout_ms=30000,
                    acks="all",
                )
                logger.info(f"✅ Connected to Kafka: {self.broker}")
                return
            except NoBrokersAvailable:
                logger.warning(f"No broker yet. Waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Connection error: {e}")
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(f"Failed to connect after {MAX_RETRIES} attempts")
                time.sleep(RETRY_DELAY)

    @staticmethod
    def build_message(row: pd.Series) -> dict:
        return {
            "Id":                     str(row.get("Id",      "") or ""),
            "ProductId":              str(row.get("ProductId","") or ""),
            "UserId":                 str(row.get("UserId",  "") or ""),
            "ProfileName":            str(row.get("ProfileName", "") or ""),
            "HelpfulnessNumerator":   int(row.get("HelpfulnessNumerator",   0) or 0),
            "HelpfulnessDenominator": int(row.get("HelpfulnessDenominator", 0) or 0),
            "Score":                  int(row.get("Score", 0) or 0),
            "Time":                   int(row.get("Time",  0) or 0),
            "Summary":                str(row.get("Summary","") or ""),
            "Text":                   str(row.get("Text",   "") or ""),
            "sentiment":              str(row.get("sentiment", "") or ""),
            "timestamp":              datetime.now(tz=timezone.utc).isoformat(),
        }

    def load_data(self) -> pd.DataFrame:
        if not Path(self.csv_path).exists():
            raise FileNotFoundError(
                f"CSV not found: {self.csv_path}\n"
                "Run the training notebook first to generate data/test_reviews.csv"
            )
        df = pd.read_csv(self.csv_path)
        required = ["ProductId", "Text", "Score"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        logger.info(f"📂 Loaded {len(df):,} reviews from {self.csv_path}")
        return df

    def stream(self, loop: bool = True) -> None:
        df = self.load_data()
        total_sent = 0
        pass_num   = 1

        while self._running:
            logger.info(f"📤 Pass #{pass_num} — streaming {len(df):,} reviews")

            for _, row in df.iterrows():
                if not self._running:
                    break

                try:
                    msg = self.build_message(row)
                    key = msg["ProductId"]

                    future = self.producer.send(self.topic, key=key, value=msg)
                    future.add_errback(lambda exc: logger.error(f"Delivery failed: {exc}"))

                    total_sent += 1
                    if total_sent % 100 == 0:
                        logger.info(
                            f"📨 Sent {total_sent:,} | ProductId={key} | "
                            f"Score={msg['Score']}"
                        )

                    time.sleep(self.delay)

                except KafkaError as e:
                    logger.error(f"Kafka error: {e}")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")

            self.producer.flush()
            logger.info(f"✅ Pass #{pass_num} done. Total sent: {total_sent:,}")

            if not loop:
                break
            pass_num += 1
            logger.info("🔄 Restarting loop...")

    def _shutdown(self, signum, frame):
        logger.info("⚠️ Shutdown received. Flushing...")
        self._running = False
        if self.producer:
            self.producer.flush(timeout=10)
            self.producer.close()
        logger.info("✅ Producer shut down.")
        sys.exit(0)

    def close(self):
        if self.producer:
            self.producer.flush(timeout=10)
            self.producer.close()


def main():
    parser = argparse.ArgumentParser(description="Amazon Review Kafka Producer")
    parser.add_argument("--broker",  default=KAFKA_BROKER)
    parser.add_argument("--topic",   default=KAFKA_TOPIC)
    parser.add_argument("--csv",     default=CSV_PATH)
    parser.add_argument("--delay",   type=float, default=PRODUCER_DELAY)
    parser.add_argument("--no-loop", action="store_true")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Amazon Review Kafka Producer")
    logger.info(f"  Broker: {args.broker} | Topic: {args.topic}")
    logger.info(f"  CSV:    {args.csv}   | Delay: {args.delay}s")
    logger.info("=" * 60)

    p = AmazonReviewProducer(args.broker, args.topic, args.delay, args.csv)
    try:
        p.connect()
        p.stream(loop=not args.no_loop)
    except (FileNotFoundError, RuntimeError) as e:
        logger.critical(str(e))
        sys.exit(1)
    finally:
        p.close()


if __name__ == "__main__":
    main()
