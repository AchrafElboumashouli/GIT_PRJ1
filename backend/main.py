"""
backend/main.py
================
FastAPI REST API for Amazon Review Sentiment Analysis.

Endpoints:
  GET /health                  → service health check
  GET /predictions             → paginated recent predictions
  GET /stats                   → overall sentiment statistics
  GET /product/{id}            → stats for a specific product
  GET /sentiment-distribution  → sentiment breakdown (global or by product)
  GET /predictions-by-date     → monthly sentiment trends
  GET /top-products            → top products by sentiment
"""

import os
import sys
import logging
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("api")

# ---------------------------------------------------------------------------
# MongoDB client (lazy import to avoid import errors during testing)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.mongo_client import MongoDBClient, MONGO_URI, MONGO_DATABASE

db_client: Optional[MongoDBClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    global db_client
    logger.info("Starting up — connecting to MongoDB...")
    try:
        db_client = MongoDBClient(MONGO_URI, MONGO_DATABASE).connect()
        logger.info("✅ MongoDB connected")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
    yield
    if db_client:
        db_client.close()
    logger.info("✅ Server shut down")


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Amazon Review Sentiment API",
    description="Real-time sentiment analysis for Amazon product reviews",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependency helper
# ---------------------------------------------------------------------------
def get_db() -> MongoDBClient:
    if db_client is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    return db_client


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint."""
    db = get_db()
    db_health = db.get_health()
    return {
        "api": "healthy",
        "version": "1.0.0",
        "database": db_health,
    }


@app.get("/predictions", tags=["Predictions"])
async def get_predictions(
    limit: int = Query(default=50, ge=1, le=500, description="Number of records"),
    skip:  int = Query(default=0, ge=0, description="Records to skip"),
    product_id: Optional[str] = Query(default=None, description="Filter by ProductId"),
):
    """
    Retrieve the most recent predictions.

    - **limit**: Max records to return (1–500)
    - **skip**: Offset for pagination
    - **product_id**: Optional filter
    """
    db = get_db()
    try:
        if product_id:
            data = db.get_prediction_by_product(product_id, limit=limit)
        else:
            data = db.get_recent_predictions(limit=limit, skip=skip)
        return {"count": len(data), "predictions": data}
    except Exception as e:
        logger.error(f"Error fetching predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", tags=["Analytics"])
async def get_stats():
    """
    Overall statistics: total reviews, sentiment counts, percentages.
    """
    db = get_db()
    try:
        return db.get_overall_stats()
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/product/{product_id}", tags=["Analytics"])
async def get_product_stats(product_id: str):
    """
    Detailed analytics for a specific product.

    Example: `/product/B001E4KFG0`
    """
    db = get_db()
    try:
        data = db.get_product_stats(product_id)
        if data["total_reviews"] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No predictions found for product: {product_id}"
            )
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching product stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sentiment-distribution", tags=["Analytics"])
async def get_sentiment_distribution(
    product_id: Optional[str] = Query(default=None, description="Filter by ProductId")
):
    """
    Sentiment distribution (global or per product).
    Returns counts and percentages for positive / neutral / negative.
    """
    db = get_db()
    try:
        return db.get_sentiment_distribution(product_id)
    except Exception as e:
        logger.error(f"Error fetching distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predictions-by-date", tags=["Analytics"])
async def get_predictions_by_date():
    """
    Monthly sentiment trends — suitable for time-series charts.
    Returns aggregated counts grouped by month (YYYY-MM).
    """
    db = get_db()
    try:
        raw = db.get_predictions_by_date()
        # Reformat for frontend consumption
        result: dict = {}
        for row in raw:
            date       = row["_id"]["date"]
            prediction = row["_id"]["prediction"]
            count      = row["count"]
            if date not in result:
                result[date] = {"date": date, "positive": 0, "neutral": 0, "negative": 0}
            if prediction in result[date]:
                result[date][prediction] = count

        return {"data": sorted(result.values(), key=lambda x: x["date"])}
    except Exception as e:
        logger.error(f"Error fetching predictions by date: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/top-products", tags=["Analytics"])
async def get_top_products(
    sentiment: str = Query(default="positive", regex="^(positive|neutral|negative)$"),
    limit: int     = Query(default=10, ge=1, le=50),
):
    """
    Top products by sentiment count.

    - **sentiment**: positive | neutral | negative
    - **limit**: Number of products to return
    """
    db = get_db()
    try:
        data = db.get_top_products(sentiment=sentiment, limit=limit)
        return {"sentiment": sentiment, "products": data}
    except Exception as e:
        logger.error(f"Error fetching top products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Entry point (for local dev)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=os.getenv("API_DEBUG", "false").lower() == "true",
        log_level="info",
    )
