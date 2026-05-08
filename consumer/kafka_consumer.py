"""
consumer/kafka_consumer.py
===========================
Lightweight Python consumer replacing Spark Structured Streaming.

Pipeline:
  Kafka topic → JSON parse → VADER Sentiment Analysis → MongoDB

No ML model files required — uses VADER (rule-based, works instantly).
Falls back to TextBlob if VADER unavailable.
"""

import os
import sys
import json
import time
import signal
import logging
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, BulkWriteError

# ---------------------------------------------------------------------------
# VADER Sentiment (no model files needed)
# ---------------------------------------------------------------------------
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _ANALYZER = SentimentIntensityAnalyzer()
    _SENTIMENT_ENGINE = "vader"
except ImportError:
    try:
        from textblob import TextBlob
        _ANALYZER = None
        _SENTIMENT_ENGINE = "textblob"
    except ImportError:
        _ANALYZER = None
        _SENTIMENT_ENGINE = "fallback"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/consumer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("kafka_consumer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER     = os.getenv("KAFKA_BROKER",       "localhost:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC",         "reviews-topic")
CONSUMER_GROUP   = os.getenv("CONSUMER_GROUP_ID",   "sentiment-consumer-group")
MONGO_URI        = os.getenv("MONGO_URI",            "mongodb://localhost:27017/reviews_db")
MONGO_DATABASE   = os.getenv("MONGO_DATABASE",       "reviews_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_PREDICTIONS", "predictions")
MAX_RETRIES      = int(os.getenv("KAFKA_MAX_RETRIES", "15"))
RETRY_DELAY      = int(os.getenv("KAFKA_RETRY_DELAY", "5"))
BATCH_SIZE       = int(os.getenv("CONSUMER_BATCH_SIZE", "10"))


# ---------------------------------------------------------------------------
# Sentiment Analysis
# ---------------------------------------------------------------------------
def analyze_sentiment(summary: str, text: str) -> str:
    """
    Analyze sentiment of a review using VADER (primary) or TextBlob (fallback).

    Returns: 'positive', 'neutral', or 'negative'
    """
    combined = f"{summary or ''} {text or ''}".strip()
    if not combined:
        return "neutral"

    try:
        if _SENTIMENT_ENGINE == "vader":
            scores = _ANALYZER.polarity_scores(combined)
            compound = scores["compound"]
            if compound >= 0.05:
                return "positive"
            elif compound <= -0.05:
                return "negative"
            else:
                return "neutral"

        elif _SENTIMENT_ENGINE == "textblob":
            from textblob import TextBlob
            polarity = TextBlob(combined).sentiment.polarity
            if polarity > 0.05:
                return "positive"
            elif polarity < -0.05:
                return "negative"
            else:
                return "neutral"

        else:
            # Ultimate fallback: use Score field if available
            return "neutral"

    except Exception as e:
        logger.warning(f"Sentiment analysis error: {e}")
        return "neutral"


def score_to_sentiment(score: int) -> str:
    """Convert numeric score to sentiment label as ground truth reference."""
    if score and score > 3:
        return "positive"
    elif score and score == 3:
        return "neutral"
    elif score and score < 3:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# MongoDB Setup
# ---------------------------------------------------------------------------
def connect_mongo(uri: str, db_name: str, col_name: str):
    """Connect to MongoDB and return the predictions collection."""
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[db_name]
    col = db[col_name]

    # Create indexes (idempotent)
    col.create_index([("ProductId",   ASCENDING)])
    col.create_index([("timestamp",   DESCENDING)])
    col.create_index([("prediction",  ASCENDING)])
    col.create_index([("Score",       ASCENDING)])
    col.create_index([("review_date", ASCENDING)])
    col.create_index([("ProductId",   ASCENDING), ("prediction", ASCENDING)])

    logger.info(f"✅ MongoDB connected: {uri} / {db_name}.{col_name}")
    return col


def insert_batch(col, documents: list) -> int:
    """Insert a batch of documents into MongoDB. Returns inserted count."""
    if not documents:
        return 0
    try:
        result = col.insert_many(documents, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
        logger.warning(f"Partial write: {inserted}/{len(documents)} inserted")
        return inserted
    except Exception as e:
        logger.error(f"MongoDB insert error: {e}")
        return 0


# ---------------------------------------------------------------------------
# Main Consumer
# ---------------------------------------------------------------------------
class SentimentConsumer:
    """Kafka consumer that performs sentiment analysis and writes to MongoDB."""

    def __init__(self):
        self.consumer: Optional[KafkaConsumer] = None
        self.mongo_col = None
        self._running = True
        self.stats = {"total": 0, "positive": 0, "neutral": 0, "negative": 0, "errors": 0}

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def connect_kafka(self) -> None:
        """Connect to Kafka with retry logic."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Connecting to Kafka at {KAFKA_BROKER} (attempt {attempt}/{MAX_RETRIES})")
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=[KAFKA_BROKER],
                    group_id=CONSUMER_GROUP,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=-1,  # block forever
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    max_poll_records=BATCH_SIZE,
                    max_poll_interval_ms=300000,
                )
                logger.info(f"✅ Connected to Kafka | topic: {KAFKA_TOPIC} | group: {CONSUMER_GROUP}")
                return
            except NoBrokersAvailable:
                logger.warning(f"No brokers available. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Kafka connection error: {e}")
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(f"Failed to connect after {MAX_RETRIES} attempts")
                time.sleep(RETRY_DELAY)

    def connect_mongo(self) -> None:
        """Connect to MongoDB with retry logic."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.mongo_col = connect_mongo(MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION)
                return
            except Exception as e:
                logger.warning(f"MongoDB not ready (attempt {attempt}): {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
        raise RuntimeError("Failed to connect to MongoDB")

    def process_message(self, raw: dict) -> Optional[dict]:
        """Process a single Kafka message into a MongoDB document."""
        try:
            summary   = str(raw.get("Summary", "") or "")
            text      = str(raw.get("Text",    "") or "")
            score     = raw.get("Score", 0)

            prediction = analyze_sentiment(summary, text)

            # Convert unix timestamp to readable date
            unix_time = raw.get("Time", 0)
            try:
                review_date = datetime.fromtimestamp(int(unix_time), tz=timezone.utc).strftime("%Y-%m-%d")
            except (ValueError, TypeError, OSError):
                review_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

            doc = {
                "Id":                     str(raw.get("Id",      "") or ""),
                "ProductId":              str(raw.get("ProductId","") or ""),
                "UserId":                 str(raw.get("UserId",   "") or ""),
                "ProfileName":            str(raw.get("ProfileName", "") or ""),
                "HelpfulnessNumerator":   int(raw.get("HelpfulnessNumerator",   0) or 0),
                "HelpfulnessDenominator": int(raw.get("HelpfulnessDenominator", 0) or 0),
                "Score":                  int(score or 0),
                "Time":                   int(unix_time or 0),
                "review_date":            review_date,
                "Summary":                summary[:500],  # cap length
                "Text":                   text[:2000],    # cap length
                "original_sentiment":     str(raw.get("sentiment", "") or ""),
                "prediction":             prediction,
                "sentiment_engine":       _SENTIMENT_ENGINE,
                "timestamp":              raw.get("timestamp", datetime.now(tz=timezone.utc).isoformat()),
                "processed_at":           datetime.now(tz=timezone.utc).isoformat(),
            }
            return doc

        except Exception as e:
            logger.error(f"Error processing message: {e} | raw keys: {list(raw.keys())}")
            self.stats["errors"] += 1
            return None

    def run(self) -> None:
        """Main consumer loop."""
        logger.info(f"🚀 Starting consumer | engine: {_SENTIMENT_ENGINE.upper()}")
        batch = []
        last_flush = time.time()

        for message in self.consumer:
            if not self._running:
                break

            try:
                raw = message.value
                doc = self.process_message(raw)

                if doc:
                    batch.append(doc)
                    self.stats["total"]             += 1
                    self.stats[doc["prediction"]]   += 1

            except Exception as e:
                logger.error(f"Message loop error: {e}")
                self.stats["errors"] += 1

            # Flush batch every BATCH_SIZE messages or every 5 seconds
            now = time.time()
            if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= 5.0):
                inserted = insert_batch(self.mongo_col, batch)
                logger.info(
                    f"📥 Flushed {inserted}/{len(batch)} docs | "
                    f"Total: {self.stats['total']:,} | "
                    f"✅ {self.stats['positive']} 🔶 {self.stats['neutral']} ❌ {self.stats['negative']}"
                )
                batch.clear()
                last_flush = now

        # Final flush
        if batch:
            insert_batch(self.mongo_col, batch)
            logger.info(f"Final flush: {len(batch)} docs")

    def _shutdown(self, signum, frame) -> None:
        logger.info("⚠️ Shutdown signal received. Closing consumer...")
        self._running = False
        if self.consumer:
            self.consumer.close()
        logger.info("✅ Consumer shut down cleanly.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("  Amazon Review Sentiment — Python Consumer")
    logger.info(f"  Engine:  {_SENTIMENT_ENGINE.upper()}")
    logger.info(f"  Kafka:   {KAFKA_BROKER}")
    logger.info(f"  Topic:   {KAFKA_TOPIC}")
    logger.info(f"  Group:   {CONSUMER_GROUP}")
    logger.info(f"  MongoDB: {MONGO_URI}")
    logger.info("=" * 60)

    svc = SentimentConsumer()
    svc.connect_mongo()
    svc.connect_kafka()
    svc.run()


if __name__ == "__main__":
    main()
