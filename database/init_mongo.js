// database/init_mongo.js
// ============================================================
// MongoDB initialization script
// Creates database, collections, and indexes on first startup
// ============================================================

db = db.getSiblingDB('reviews-topic');

// Create collections
db.createCollection('predictions');
db.createCollection('stats');

// Create indexes on predictions collection
db.predictions.createIndex({ "ProductId":  1 });
db.predictions.createIndex({ "timestamp":  -1 });
db.predictions.createIndex({ "prediction": 1 });
db.predictions.createIndex({ "Score":      1 });
db.predictions.createIndex({ "review_date": 1 });
db.predictions.createIndex({ "ProductId": 1, "prediction": 1 });

print("✅ MongoDB initialized: reviews-topic database with indexes");
