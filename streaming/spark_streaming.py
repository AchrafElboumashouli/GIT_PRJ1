"""
streaming/spark_streaming.py
=============================
Spark Structured Streaming consumer for Amazon review sentiment analysis.

Pipeline:
  Kafka topic → JSON parse → Text preprocessing → ML prediction → MongoDB

Uses the trained sklearn model (joblib) for predictions.
"""

import os
import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime

import joblib
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, LongType
)

# ---------------------------------------------------------------------------
# Download NLTK data (runs once)
# ---------------------------------------------------------------------------
nltk.download("stopwords", quiet=True)
nltk.download("punkt",     quiet=True)
nltk.download("wordnet",   quiet=True)
nltk.download("punkt_tab", quiet=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_streaming")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER       = os.getenv("KAFKA_BROKER",       "localhost:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",         "reviews-topic")
MONGO_URI          = os.getenv("MONGO_URI",           "mongodb://localhost:27017/reviews-topic")
MONGO_DATABASE     = os.getenv("MONGO_DATABASE",      "reviews-topic")
MONGO_COLLECTION   = os.getenv("MONGO_COLLECTION_PREDICTIONS", "predictions")
MODEL_PATH         = os.getenv("MODEL_PATH",          "model/best_model.pkl")
VECTORIZER_PATH    = os.getenv("VECTORIZER_PATH",     "model/tfidf_vectorizer.pkl")
LABEL_ENCODER_PATH = os.getenv("LABEL_ENCODER_PATH",  "model/label_encoder.pkl")
CHECKPOINT_DIR     = os.getenv("CHECKPOINT_DIR",      "/tmp/spark_checkpoint")
TRIGGER_INTERVAL   = os.getenv("TRIGGER_INTERVAL",    "10 seconds")

# ---------------------------------------------------------------------------
# Kafka message schema
# ---------------------------------------------------------------------------
REVIEW_SCHEMA = StructType([
    StructField("Id",                     StringType(), True),
    StructField("ProductId",              StringType(), True),
    StructField("UserId",                 StringType(), True),
    StructField("ProfileName",            StringType(), True),
    StructField("HelpfulnessNumerator",   IntegerType(), True),
    StructField("HelpfulnessDenominator", IntegerType(), True),
    StructField("Score",                  IntegerType(), True),
    StructField("Time",                   LongType(),   True),
    StructField("Summary",                StringType(), True),
    StructField("Text",                   StringType(), True),
    StructField("sentiment",              StringType(), True),
    StructField("timestamp",              StringType(), True),
])


# ---------------------------------------------------------------------------
# Text preprocessing (mirrors the training notebook)
# ---------------------------------------------------------------------------
class TextPreprocessor:
    """NLP preprocessing pipeline matching training configuration."""

    def __init__(self):
        self.stop_words = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()

    def preprocess(self, text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return ""
        text = text.lower()
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"http\S+|www\S+", " ", text)
        text = re.sub(r"[^a-z\s]", " ", text)
        tokens = word_tokenize(text)
        tokens = [t for t in tokens if t not in self.stop_words and len(t) > 2]
        tokens = [self.lemmatizer.lemmatize(t) for t in tokens]
        return " ".join(tokens)


# ---------------------------------------------------------------------------
# MongoDB writer
# ---------------------------------------------------------------------------
def write_to_mongo(df, epoch_id):
    """
    Write a micro-batch DataFrame to MongoDB.
    Called by Spark's foreachBatch.
    """
    try:
        from pymongo import MongoClient, ASCENDING
        from pymongo.errors import BulkWriteError

        rows = df.collect()
        if not rows:
            logger.info(f"Epoch {epoch_id}: empty batch, skipping")
            return

        client = MongoClient(MONGO_URI)
        db     = client[MONGO_DATABASE]
        col    = db[MONGO_COLLECTION]

        # Create indexes (idempotent)
        col.create_index([("ProductId", ASCENDING)])
        col.create_index([("timestamp", ASCENDING)])
        col.create_index([("prediction", ASCENDING)])

        documents = [row.asDict() for row in rows]
        col.insert_many(documents, ordered=False)

        logger.info(
            f"Epoch {epoch_id}: inserted {len(documents)} documents "
            f"into MongoDB ({MONGO_DATABASE}.{MONGO_COLLECTION})"
        )
        client.close()

    except BulkWriteError as bwe:
        logger.warning(f"Epoch {epoch_id}: partial write error: {bwe.details}")
    except Exception as e:
        logger.error(f"Epoch {epoch_id}: MongoDB write failed: {e}")


# ---------------------------------------------------------------------------
# Main streaming job
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """Create and configure SparkSession with Kafka & MongoDB connectors."""
    return (
        SparkSession.builder
        .appName("AmazonReviewSentimentStreaming")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        # Kafka connector
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"
        )
        # MongoDB settings
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", MONGO_DATABASE)
        .config("spark.mongodb.write.collection", MONGO_COLLECTION)
        # Performance settings
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_ml_artifacts():
    """Load trained model, vectorizer, and label encoder from disk."""
    logger.info("Loading ML artifacts...")
    for path in [MODEL_PATH, VECTORIZER_PATH, LABEL_ENCODER_PATH]:
        if not Path(path).exists():
            raise FileNotFoundError(
                f"ML artifact not found: {path}\n"
                "Please run the preprocessing notebook first."
            )

    model    = joblib.load(MODEL_PATH)
    tfidf    = joblib.load(VECTORIZER_PATH)
    le       = joblib.load(LABEL_ENCODER_PATH)

    logger.info(f"✅ Loaded model:     {MODEL_PATH}")
    logger.info(f"✅ Loaded vectorizer: {VECTORIZER_PATH}")
    logger.info(f"✅ Loaded encoder:    {LABEL_ENCODER_PATH}")
    return model, tfidf, le


def run_streaming():
    """Main entry point for the Spark Structured Streaming job."""

    # Load ML artifacts (broadcast to all executors in real cluster)
    model, tfidf, le = load_ml_artifacts()
    preprocessor = TextPreprocessor()

    # UDF for preprocessing + prediction (runs on the driver in local mode)
    def predict_sentiment(summary: str, text: str) -> str:
        """Predict sentiment for a single review."""
        try:
            combined = f"{summary or ''} {text or ''}"
            clean = preprocessor.preprocess(combined)
            if not clean:
                return "neutral"
            vec = tfidf.transform([clean])
            pred = model.predict(vec)[0]
            return le.inverse_transform([pred])[0]
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return "neutral"

    predict_udf = F.udf(predict_sentiment, StringType())

    # --------------- Spark Session ---------------
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"✅ Spark version: {spark.version}")

    # --------------- Read from Kafka ---------------
    logger.info(f"Connecting to Kafka: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

    # --------------- Parse JSON ---------------
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                REVIEW_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    # --------------- Apply ML Prediction ---------------
    predicted = (
        parsed
        .withColumn("prediction", predict_udf(F.col("Summary"), F.col("Text")))
        .withColumn("processed_at", F.current_timestamp().cast("string"))
        .withColumn("review_date",
            F.from_unixtime(F.col("Time")).cast("string")
        )
        .select(
            "Id", "ProductId", "UserId", "ProfileName",
            "HelpfulnessNumerator", "HelpfulnessDenominator",
            "Score", "Time", "review_date",
            "Summary", "Text", "sentiment",
            "prediction", "timestamp", "processed_at"
        )
    )

    # --------------- Write to MongoDB ---------------
    logger.info("Starting streaming query → MongoDB...")

    query = (
        predicted.writeStream
        .foreachBatch(write_to_mongo)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    logger.info("✅ Streaming started. Waiting for data...")
    logger.info(f"   Checkpoint: {CHECKPOINT_DIR}")
    logger.info(f"   Trigger:    {TRIGGER_INTERVAL}")

    query.awaitTermination()


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  Amazon Review Sentiment — Spark Streaming")
    logger.info("=" * 60)
    logger.info(f"  Kafka:   {KAFKA_BROKER}")
    logger.info(f"  Topic:   {KAFKA_TOPIC}")
    logger.info(f"  MongoDB: {MONGO_URI}")
    logger.info("=" * 60)

    run_streaming()
