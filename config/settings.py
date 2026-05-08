"""
config/settings.py
==================
Centralized configuration management using environment variables.
All services import from here for consistency.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class KafkaConfig:
    broker: str = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "reviews-topic")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "spark-consumer-group")
    num_partitions: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


@dataclass
class SparkConfig:
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    app_name: str = os.getenv("SPARK_APP_NAME", "AmazonReviewSentiment")
    streaming_interval: int = int(os.getenv("SPARK_STREAMING_INTERVAL", "10"))
    kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


@dataclass
class MongoConfig:
    host: str = os.getenv("MONGO_HOST", "localhost")
    port: int = int(os.getenv("MONGO_PORT", "27017"))
    database: str = os.getenv("MONGO_DATABASE", "reviews-topic")
    collection_predictions: str = os.getenv("MONGO_COLLECTION_PREDICTIONS", "predictions")
    collection_stats: str = os.getenv("MONGO_COLLECTION_STATS", "stats")
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/reviews-topic")

    @property
    def connection_string(self) -> str:
        return f"mongodb://{self.host}:{self.port}/"


@dataclass
class APIConfig:
    host: str = os.getenv("API_HOST", "0.0.0.0")
    port: int = int(os.getenv("API_PORT", "8000"))
    debug: bool = os.getenv("API_DEBUG", "false").lower() == "true"


@dataclass
class ModelConfig:
    model_path: str = os.getenv("MODEL_PATH", "model/best_model.pkl")
    vectorizer_path: str = os.getenv("VECTORIZER_PATH", "model/tfidf_vectorizer.pkl")
    label_encoder_path: str = os.getenv("LABEL_ENCODER_PATH", "model/label_encoder.pkl")
    spark_model_path: str = os.getenv("SPARK_MODEL_PATH", "model/spark_pipeline")


@dataclass
class ProducerConfig:
    delay: float = float(os.getenv("PRODUCER_DELAY", "2"))
    csv_path: str = os.getenv("CSV_PATH", "data/Reviews.csv")
    batch_size: int = int(os.getenv("PRODUCER_BATCH_SIZE", "1"))


@dataclass
class MLflowConfig:
    tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    experiment: str = os.getenv("MLFLOW_EXPERIMENT", "amazon-sentiment")


@dataclass
class Settings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    api: APIConfig = field(default_factory=APIConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    mlflow: MLflowConfig = field(default_factory=MLflowConfig)

    # Label mappings
    LABEL_MAP: dict = field(default_factory=lambda: {
        0: "negative",
        1: "neutral",
        2: "positive"
    })

    SENTIMENT_COLORS: dict = field(default_factory=lambda: {
        "positive": "#2ECC71",
        "neutral": "#F39C12",
        "negative": "#E74C3C"
    })


# Global settings instance
settings = Settings()
