# Feature Store Operations Runbook

## Quick Start

```bash
cd feature-store
cp .env.example .env
# Edit .env: set ANTHROPIC_API_KEY
make up
```

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Feature Store API | http://localhost:8000 | — |
| API Docs (Swagger) | http://localhost:8000/docs | — |
| Spark UI | http://localhost:8888 | — |
| Airflow | http://localhost:8080 | admin / admin |
| Monitoring Dashboard | http://localhost:8501 | — |

## Common Diagnostics

### Check Kafka consumer lag
```bash
make kafka-lag
```

### Check Redis feature freshness
```bash
docker compose exec redis redis-cli
> TTL features:usr_00001
> HGETALL features:usr_00001
```

### Check API health
```bash
curl http://localhost:8000/health | jq .
```

### Run consistency check manually
```bash
# Trigger via Airflow or directly
curl -X POST http://localhost:8000/diagnostics/rca \
  -H "Content-Type: application/json" \
  -d '{"feature_name":"avg_spend_1h","psi_score":0.25,"ks_statistic":0.15,"z_score":3.8,"severity":"alert"}'
```

### View streaming job logs
```bash
make logs-streaming
```

### Run latency benchmark (requires `hey`)
```bash
make bench-api
```

## Alerts Reference

| Alert | Condition | Action |
|---|---|---|
| Feature staleness | freshness_seconds > 300 | Check Spark streaming logs |
| Consistency failure | inconsistency_rate > 5% | Run drift monitoring DAG + RCA |
| PSI alert | PSI > 0.2 | Investigate upstream data + trigger RCA |
| KS significance | p-value < 0.05 | Check for schema changes, run RCA |
| API latency | p99 > 50ms | Check Redis connection pool, Spark lag |

## Feature Refresh

The `feature_store_refresh` Airflow DAG runs every 5 minutes and extends TTL for keys with < 60s remaining. To force a refresh for a specific user:

```bash
curl -X POST http://localhost:8000/refresh-feature \
  -H "Content-Type: application/json" \
  -d '{"user_id":"usr_00001"}'
```
