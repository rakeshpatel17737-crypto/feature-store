# Architecture: Enterprise Real-Time Feature Store

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                       │
│  E-commerce Events: page_view | add_to_cart | purchase | search | abandon │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ ~100 events/sec
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     KAFKA (Confluent 7.6.0)                               │
│  Topics:                                                                   │
│  • ecommerce.events.raw       (6 partitions, 1-week retention)            │
│  • ecommerce.features.computed (6 partitions)                             │
│  • ecommerce.alerts.drift      (1 partition)                              │
│  • ecommerce.features.refresh  (3 partitions)                             │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │ PySpark readStream (Kafka source)
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              PYSPARK STRUCTURED STREAMING (Spark 3.5.1)                   │
│                                                                            │
│  Watermark: 10 minutes | Trigger: every 30 seconds                        │
│                                                                            │
│  Features computed (from feature_definitions.py — single source of truth):│
│  • txn_count_5m          (tumbling 5-min window)                          │
│  • avg_spend_1h          (sliding 1h/5min window)                         │
│  • session_activity_rate (tumbling 5-min window)                          │
│  • cart_abandon_ratio    (tumbling 30-min window)                         │
│  • product_interaction_freq (tumbling 1h window)                          │
│  • anomaly_score         (Z-score vs 24h baseline)                        │
└────────────┬──────────────────────────────────┬───────────────────────────┘
             │ foreachBatch                      │ foreachBatch
             ▼                                   ▼
┌────────────────────────┐        ┌─────────────────────────────────────┐
│  REDIS (Online Store)  │        │  DELTA LAKE (Offline Store)          │
│                         │        │                                      │
│  Key: features:{user}  │        │  Path: /data/feature_store/          │
│  Type: HASH            │        │  Partitions: event_date + user_prefix │
│  TTL: 300 seconds      │        │  Format: Delta (time-travel enabled) │
│  Max mem: 512MB LRU    │        │  Retention: 30 days                  │
│                         │        │  Z-ORDER: user_id + computed_at     │
│  p99 latency: <20ms    │        │                                      │
└────────────┬────────────┘        └──────────────┬──────────────────────┘
             │                                     │
             │                      ┌──────────────┘
             ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    FASTAPI SERVING API (:8000)                            │
│                                                                            │
│  GET  /features/{user_id}   → HGETALL → FeatureVector (p99 <50ms)        │
│  GET  /health               → Redis + DB health check                    │
│  GET  /feature-metadata     → Feature Registry (PostgreSQL)              │
│  POST /refresh-feature      → Extend TTL, queue recompute                │
│  POST /diagnostics/rca      → LLM Root Cause Analysis                   │
└────────────────────────────────────────┬─────────────────────────────────┘
                                         │
              ┌──────────────────────────┤
              │                          │
              ▼                          ▼
┌─────────────────────────┐  ┌──────────────────────────────────────────┐
│  CONSISTENCY VALIDATOR  │  │  FEATURE REGISTRY (PostgreSQL)            │
│                          │  │                                           │
│  Sample 1000 users       │  │  Tables: feature_definitions             │
│  Compare offline/online  │  │          feature_lineage                 │
│  Threshold: 1% deviation │  │          feature_metrics                 │
│  Alert on: >5% skew      │  │          rca_results                    │
└──────────┬───────────────┘  └──────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     DRIFT DETECTOR                                         │
│                                                                            │
│  PSI: < 0.1 (ok) | 0.1-0.2 (warn) | > 0.2 (alert)                       │
│  KS test: scipy.stats.ks_2samp (α=0.05)                                  │
│  Z-score: |z| > 3.0 → anomalous                                          │
│  KL divergence: binned probability divergence                             │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │ DriftReport
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              LLM ROOT CAUSE ANALYSIS (Claude API)                         │
│                                                                            │
│  Model: claude-sonnet-4-5                                                 │
│  Tool use: submit_rca_diagnosis (structured output)                       │
│  Prompt caching: system prompt cached (ephemeral)                         │
│  Fallback: rule-based diagnosis if API unavailable                        │
│                                                                            │
│  Output: probable_cause | cause_category | confidence | remediation       │
└──────────────────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┤
              │            │
              ▼            ▼
┌────────────────┐  ┌──────────────────────────────────────────────────────┐
│ AIRFLOW (:8080)│  │  STREAMLIT DASHBOARD (:8501)                          │
│                 │  │                                                       │
│ DAGs:           │  │  Pages:                                              │
│ • backfill      │  │  1. Feature Freshness (age distribution)             │
│ • consistency   │  │  2. Latency Metrics (p50/p95/p99)                    │
│   (@15min)      │  │  3. Drift Report (PSI time-series)                   │
│ • drift_monitor │  │  4. Consistency Checks (offline vs online)           │
│   (@hourly)     │  │  5. Throughput (events/sec, pipeline health)         │
│ • refresh       │  │                                                       │
│   (@5min)       │  └──────────────────────────────────────────────────────┘
└────────────────┘
```

## Key Design Decisions

### Training-Serving Skew Prevention
`streaming/feature_definitions.py` is the single authoritative source of feature computation logic. Both the streaming job (online path) and the backfill job (offline path) import from this module. This guarantees that `offline_value == online_value` for the same input data, which is the core invariant the system enforces.

### Sub-50ms Online Retrieval
All user features are stored in a single Redis HASH (`features:{user_id}`). One `HGETALL` command retrieves the complete feature vector in a single round-trip, achieving p99 < 20ms on local Docker and < 50ms in production.

### Structured LLM Output
Claude's tool use API (`tool_choice: forced`) guarantees structured JSON output without fragile string parsing. The tool schema matches the `RCADiagnosis` Pydantic model exactly.

### Prompt Caching
The LLM system prompt (describing the ML infra expert persona and all diagnostic context) is cached using Anthropic's `cache_control: ephemeral`. This saves ~$0.003 per RCA call for the repeated system prompt tokens.
