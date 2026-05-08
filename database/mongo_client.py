"""
database/mongo_client.py
=========================
MongoDB client wrapper with all CRUD operations, indexing,
and analytics queries for the Amazon review sentiment system.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI      = os.getenv("MONGO_URI",    "mongodb://localhost:27017/reviews_db")
MONGO_HOST     = os.getenv("MONGO_HOST",   "localhost")
MONGO_PORT     = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "reviews-topic")
MONGO_COL_PRED = os.getenv("MONGO_COLLECTION_PREDICTIONS", "predictions")
MONGO_COL_STAT = os.getenv("MONGO_COLLECTION_STATS",       "stats")


# ---------------------------------------------------------------------------
# MongoDB Client
# ---------------------------------------------------------------------------
class MongoDBClient:
    """
    Thread-safe MongoDB client for the Amazon review sentiment system.
    Provides:
      - Collection access
      - Index management
      - CRUD operations
      - Analytics queries (for dashboard)
    """

    def __init__(self, uri: str = MONGO_URI, db_name: str = MONGO_DATABASE):
        self.uri     = uri
        self.db_name = db_name
        self._client: Optional[MongoClient] = None
        self._db: Optional[Database]        = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------
    def connect(self) -> "MongoDBClient":
        """Establish connection and set up indexes."""
        try:
            self._client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
            )
            # Test connection
            self._client.admin.command("ping")
            self._db = self._client[self.db_name]
            self._setup_indexes()
            logger.info(f"✅ Connected to MongoDB: {self.uri} / {self.db_name}")
        except ConnectionFailure as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise
        return self

    def close(self) -> None:
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Collection shortcuts
    # ------------------------------------------------------------------
    @property
    def predictions(self) -> Collection:
        return self._db[MONGO_COL_PRED]

    @property
    def stats(self) -> Collection:
        return self._db[MONGO_COL_STAT]

    # ------------------------------------------------------------------
    # Index setup
    # ------------------------------------------------------------------
    def _setup_indexes(self) -> None:
        """Create indexes for efficient querying."""
        pred = self.predictions
        pred.create_index([("ProductId",   ASCENDING)])
        pred.create_index([("timestamp",   DESCENDING)])
        pred.create_index([("prediction",  ASCENDING)])
        pred.create_index([("Score",       ASCENDING)])
        pred.create_index([("review_date", ASCENDING)])
        pred.create_index([("ProductId", ASCENDING), ("prediction", ASCENDING)])
        logger.debug("Indexes created/verified")

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------
    def insert_prediction(self, document: Dict[str, Any]) -> str:
        """Insert a single prediction document."""
        document["inserted_at"] = datetime.utcnow().isoformat()
        result = self.predictions.insert_one(document)
        return str(result.inserted_id)

    def insert_many_predictions(self, documents: List[Dict]) -> int:
        """Bulk insert predictions."""
        for doc in documents:
            doc["inserted_at"] = datetime.utcnow().isoformat()
        result = self.predictions.insert_many(documents, ordered=False)
        return len(result.inserted_ids)

    def get_recent_predictions(
        self,
        limit: int = 50,
        skip: int = 0,
    ) -> List[Dict]:
        """Fetch most recent predictions ordered by timestamp."""
        cursor = (
            self.predictions.find(
                {},
                {"_id": 0, "Id": 1, "ProductId": 1, "UserId": 1,
                 "Score": 1, "Summary": 1, "prediction": 1,
                 "sentiment": 1, "timestamp": 1, "review_date": 1}
            )
            .sort("timestamp", DESCENDING)
            .skip(skip)
            .limit(limit)
        )
        return list(cursor)

    def get_prediction_by_product(self, product_id: str, limit: int = 100) -> List[Dict]:
        """Fetch predictions for a specific product."""
        cursor = (
            self.predictions.find(
                {"ProductId": product_id},
                {"_id": 0}
            )
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    # ------------------------------------------------------------------
    # Analytics queries
    # ------------------------------------------------------------------
    def get_sentiment_distribution(self, product_id: Optional[str] = None) -> Dict:
        """Overall sentiment distribution (optionally filtered by product)."""
        match_filter = {"ProductId": product_id} if product_id else {}
        pipeline = [
            {"$match": match_filter},
            {"$group": {"_id": "$prediction", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
        results = list(self.predictions.aggregate(pipeline))
        dist = {"positive": 0, "neutral": 0, "negative": 0}
        for r in results:
            if r["_id"] in dist:
                dist[r["_id"]] = r["count"]
        total = sum(dist.values())
        dist["total"] = total
        return dist

    def get_predictions_by_date(self) -> List[Dict]:
        """Aggregate sentiment counts grouped by review date."""
        pipeline = [
            {
                "$addFields": {
                    "date": {
                        "$substr": [
                            {"$ifNull": ["$review_date", "$timestamp"]},
                            0, 7   # YYYY-MM
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": {"date": "$date", "prediction": "$prediction"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.date": 1}},
        ]
        return list(self.predictions.aggregate(pipeline))

    def get_top_products(
        self,
        sentiment: str = "positive",
        limit: int = 10,
    ) -> List[Dict]:
        """Get top products by sentiment count."""
        pipeline = [
            {"$match": {"prediction": sentiment}},
            {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
            {"$sort": {"count": DESCENDING}},
            {"$limit": limit},
            {"$project": {"ProductId": "$_id", "count": 1, "_id": 0}},
        ]
        return list(self.predictions.aggregate(pipeline))

    def get_overall_stats(self) -> Dict:
        """Summary statistics for dashboard cards."""
        total        = self.predictions.count_documents({})
        positive     = self.predictions.count_documents({"prediction": "positive"})
        neutral      = self.predictions.count_documents({"prediction": "neutral"})
        negative     = self.predictions.count_documents({"prediction": "negative"})
        total_products = len(self.predictions.distinct("ProductId"))

        return {
            "total_reviews":   total,
            "positive":        positive,
            "neutral":         neutral,
            "negative":        negative,
            "total_products":  total_products,
            "positive_pct":    round(positive / total * 100, 1) if total else 0,
            "neutral_pct":     round(neutral  / total * 100, 1) if total else 0,
            "negative_pct":    round(negative / total * 100, 1) if total else 0,
        }

    def get_product_stats(self, product_id: str) -> Dict:
        """Detailed stats for a specific product."""
        dist = self.get_sentiment_distribution(product_id)
        reviews = self.get_prediction_by_product(product_id, limit=5)
        total = dist.get("total", 0)
        return {
            "ProductId":     product_id,
            "total_reviews": total,
            "positive":      dist.get("positive", 0),
            "neutral":       dist.get("neutral",  0),
            "negative":      dist.get("negative", 0),
            "positive_pct":  round(dist.get("positive", 0) / total * 100, 1) if total else 0,
            "neutral_pct":   round(dist.get("neutral",  0) / total * 100, 1) if total else 0,
            "negative_pct":  round(dist.get("negative", 0) / total * 100, 1) if total else 0,
            "sample_reviews": reviews,
        }

    def get_health(self) -> Dict:
        """Database health check."""
        try:
            self._client.admin.command("ping")
            total = self.predictions.count_documents({})
            return {
                "status":    "healthy",
                "database":  self.db_name,
                "documents": total,
                "uri":       self.uri,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------
_client_instance: Optional[MongoDBClient] = None


def get_mongo_client() -> MongoDBClient:
    """Return a connected singleton MongoDBClient."""
    global _client_instance
    if _client_instance is None:
        _client_instance = MongoDBClient().connect()
    return _client_instance
