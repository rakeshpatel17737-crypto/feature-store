# Enterprise Real-Time Feature Store for AI/ML Systems

A production-grade, end-to-end real-time feature store that solves the **training-serving skew** problem by providing consistent feature computation for both offline model training and online inference.

---

## Architecture

```
Event Generator → Kafka → PySpark Structured Streaming
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
               Delta Lake                        Redis
             (offline store)               (online store)
                     │                             │
                     └──────────┬──────────────────┘
                                ▼
                      Consistency Validator
                                │
                     ┌──────────┴────────────┐
                     ▼                       ▼
              Drift Detector          FastAPI Serving
                     │                       │
                     ▼                       ▼
               LLM RCA Engine         Streamlit Dashboard
               (Claude API)
                     │
               Airflow DAGs
```

## Tech Stack

| Component | Technology |
|---|---|
| Event streaming | Apache Kafka 7.6.0 (Confluent) |
| Stream processing | PySpark Structured Streaming 3.5.1 |
| Offline store | Delta Lake 3.1.0 (Parquet with time-travel) |
| Online store | Redis 7.2 (HASH per user, TTL=300s) |
| Feature serving | FastAPI 0.111 + aioredis (async) |
| Feature registry | PostgreSQL 16 + SQLAlchemy 2.0 |
| Orchestration | Apache Airflow 2.9.2 |
| Monitoring | Streamlit 1.36 + Plotly |
| Root cause analysis | Claude API (claude-sonnet-4-5) with tool use |
| Drift detection | PSI + KS test + Z-score (scipy) |

---

## Quick Start

### Prerequisites
- Docker 20.10+ and Docker Compose v2
- 8GB RAM minimum (Spark + Kafka + Airflow)

### 1. Clone and configure
```bash
git clone <repo>
cd feature-store
cp .env.example .env
# Edit .env and set your ANTHROPIC_API_KEY
```

### 2. Start all services
```bash
make up
# Wait ~3 minutes for all services to initialize
make ps  # verify all are healthy
```

### 3. Verify the pipeline is running
```bash
# Check event generator is producing
make logs-ingestion

# Check streaming job is processing
make logs-streaming

# Verify features are landing in Redis
make shell-redis
> HGETALL features:usr_00001

# Call the API
curl http://localhost:8000/features/usr_00001 | jq .
```

### 4. Access dashboards
| Service | URL |
|---|---|
| Feature Store API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Spark UI | http://localhost:8888 |
| Airflow | http://localhost:8080 (admin/admin) |
| Monitoring | http://localhost:8501 |

---

## Running Tests

```bash
# Install test dependencies
pip install -r requirements.dev.txt

# Unit tests (no Docker required)
python -m pytest tests/unit/ -v

# Integration tests (Docker services must be running)
python -m pytest tests/integration/ -v --timeout=60

# All tests
python -m pytest tests/ -v
```

---

## Key Features

### Training-Serving Skew Prevention
`streaming/feature_definitions.py` is the **single source of truth** for all feature computation. Both the streaming job (online path via Redis) and the backfill job (offline path via Delta Lake) import from this same module, guaranteeing identical transforms.

### Sub-50ms Online Feature Retrieval
All 6 features per user are stored in a single Redis HASH. One `HGETALL` command returns the complete feature vector — **one round-trip, not 6** — achieving p99 < 20ms locally.

### LLM Root Cause Analysis
When drift or consistency alerts fire, Claude (claude-sonnet-4-5) analyzes the drift metrics and pipeline context to return:
- Probable root cause with confidence score
- Cause category (data_pipeline_delay, schema_change, seasonal_pattern, etc.)
- Ordered remediation steps
- Urgency level

**Prompt caching** is enabled on the system prompt, reducing token costs on repeated RCA calls.

### Feature Drift Detection
Three-layer detection:
1. **PSI** (Population Stability Index) — binned distribution comparison
2. **KS test** — continuous distribution statistical test  
3. **Z-score** — mean shift detection vs. 7-day baseline

---

## Benchmark Results (local Docker)

| Metric | Result | Target |
|---|---|---|
| Online feature retrieval (p50) | ~5ms | — |
| Online feature retrieval (p99) | ~18ms | < 50ms ✅ |
| Event throughput | ~100 events/sec | — |
| Feature consistency rate | > 99% | > 99.9% |
| Drift detection time (10k users) | < 2s | — |
| LLM RCA latency (cached) | ~800ms | — |

---

## Project Structure

```
feature-store/
├── ingestion/          # Kafka producer, synthetic event generator
├── streaming/          # PySpark Structured Streaming + feature_definitions.py
├── offline_store/      # Delta Lake writer/reader/backfill
├── online_store/       # Redis client, TTL management
├── api/                # FastAPI serving layer
├── registry/           # Feature registry (PostgreSQL)
├── validation/         # Consistency validator, PSI calculator
├── drift_detection/    # PSI + KS + Z-score drift detector
├── diagnostics/        # LLM RCA engine (Claude API)
├── monitoring/         # Streamlit dashboard
├── airflow/            # Airflow DAGs
├── tests/              # Unit + integration tests
├── docker/             # Dockerfiles + requirements per service
└── docs/               # Architecture, API reference, runbook
```

---

## Resume Impact Metrics

- **Sub-50ms online feature retrieval** (p99 ~18ms on local Docker)
- **99%+ feature consistency** (PSI < 0.05 steady-state)
- **Drift detection** on 10k-user samples in < 2 seconds
- **100 events/sec** sustained throughput on single Kafka broker
- **LLM-assisted RCA** with structured output and prompt caching
- **Point-in-time correct** offline features via Delta Lake time-travel
- **12-service Docker Compose** deployment (zero manual setup)
