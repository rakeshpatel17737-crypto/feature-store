.PHONY: up down logs test test-unit test-integration build clean ps shell-api shell-redis

# ── Infrastructure ────────────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo "Services starting. Use 'make ps' to check status."

up-infra:
	docker compose up -d zookeeper kafka kafka-init postgres redis
	@echo "Core infrastructure up."

down:
	docker compose down

down-volumes:
	docker compose down -v
	@echo "All volumes removed."

build:
	docker compose build --no-cache

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=50

logs-%:
	docker compose logs -f --tail=100 $*

restart-%:
	docker compose restart $*

# ── Testing ───────────────────────────────────────────────────────────────────
test-unit:
	cd feature-store && python -m pytest tests/unit/ -v --tb=short 2>/dev/null || \
	python -m pytest tests/unit/ -v --tb=short

test-integration:
	python -m pytest tests/integration/ -v --tb=short --timeout=60

test:
	python -m pytest tests/ -v --tb=short --timeout=60

# ── Development Helpers ───────────────────────────────────────────────────────
shell-api:
	docker compose exec api bash

shell-redis:
	docker compose exec redis redis-cli

shell-kafka:
	docker compose exec kafka bash

shell-postgres:
	docker compose exec postgres psql -U featurestore -d feature_store

kafka-topics:
	docker compose exec kafka kafka-topics --list --bootstrap-server localhost:29092

kafka-lag:
	docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
		--describe --group feature-processor-cg

# ── Benchmarks ────────────────────────────────────────────────────────────────
bench-api:
	@echo "Running API latency benchmark (requires 'hey' CLI)..."
	hey -n 1000 -c 10 http://localhost:8000/features/usr_00001

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
