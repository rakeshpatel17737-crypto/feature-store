# Feature Store API Reference

Base URL: `http://localhost:8000`

## Endpoints

### GET /health
Returns system health status.

**Response 200:**
```json
{
  "status": "healthy",
  "redis": "ok",
  "db": "ok",
  "timestamp": "2026-05-31T10:23:45Z"
}
```

---

### GET /features/{user_id}
Retrieve the complete feature vector for a user. Single HGETALL → p99 < 50ms.

**Query Parameters:**
- `features` (optional): comma-separated feature names to filter
- `include_metadata` (default: true): include freshness metadata

**Response 200:**
```json
{
  "user_id": "usr_00123",
  "features": {
    "txn_count_5m": 3.0,
    "avg_spend_1h": 45.20,
    "session_activity_rate": 1.5,
    "cart_abandon_ratio": 0.25,
    "product_interaction_freq": 7.0,
    "anomaly_score": 0.12
  },
  "metadata": {
    "computed_at": "1748692800.5",
    "freshness_seconds": 42.3,
    "source": "online_store",
    "schema_version": "1.0"
  }
}
```

**Response 404:**
```json
{"detail": "No features found for user usr_00123"}
```

---

### GET /feature-metadata
List all active feature definitions from the registry.

**Response 200:** Array of FeatureDefinition objects with lineage.

---

### GET /feature-metadata/{feature_name}
Get a single feature definition with full lineage.

---

### POST /refresh-feature
Queue a TTL refresh for a user's cached features.

**Request:**
```json
{"user_id": "usr_00123", "feature_names": ["txn_count_5m"]}
```

**Response 202:**
```json
{
  "status": "refresh_queued",
  "user_id": "usr_00123",
  "queued_features": ["txn_count_5m"]
}
```

---

### POST /diagnostics/rca
Run LLM-based root cause analysis on a drift report.

**Request:**
```json
{
  "feature_name": "avg_spend_1h",
  "psi_score": 0.25,
  "ks_statistic": 0.15,
  "z_score": 3.8,
  "severity": "alert"
}
```

**Response 200:**
```json
{
  "feature_name": "avg_spend_1h",
  "probable_cause": "Feature drift likely caused by delayed Kafka ingestion from partition 3, resulting in stale features beyond TTL.",
  "cause_category": "data_pipeline_delay",
  "confidence": 0.87,
  "affected_features": ["avg_spend_1h", "txn_count_5m"],
  "remediation_steps": [
    "Check Kafka consumer lag: kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group feature-processor-cg",
    "Verify Redis key TTL: redis-cli TTL features:usr_00001",
    "Check Spark streaming micro-batch time in Spark UI at :8888"
  ],
  "urgency": "high",
  "estimated_impact": "Model accuracy degradation ~2-5% on spend-related predictions",
  "model_used": "claude-sonnet-4-5"
}
```

## Feature Definitions

| Feature | Window | Description |
|---|---|---|
| `txn_count_5m` | Tumbling 5-min | Purchase event count |
| `avg_spend_1h` | Sliding 1h/5min | Average purchase amount |
| `session_activity_rate` | Tumbling 5-min | Events per distinct session |
| `cart_abandon_ratio` | Tumbling 30-min | Cart events without purchase |
| `product_interaction_freq` | Tumbling 1h | Page view count |
| `anomaly_score` | Tumbling 5-min | Z-score vs 24h baseline |
