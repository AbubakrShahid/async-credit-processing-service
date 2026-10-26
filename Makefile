.PHONY: up down logs ps seed smoke check lint fmt typecheck alerts-check test test-unit test-integration

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

seed:
	docker compose exec api python -m credit_service.seed

smoke:
	python3 scripts/smoke_test.py

check: lint typecheck test

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

alerts-check:
	docker run --rm -v "$(CURDIR)/ops:/ops:ro" -w /ops --entrypoint promtool prom/prometheus:v3.5.0 check rules prometheus-alerts.yml
	docker run --rm -v "$(CURDIR)/ops:/ops:ro" -w /ops --entrypoint promtool prom/prometheus:v3.5.0 test rules prometheus-alerts.test.yml

test:
	uv run pytest

test-unit:
	uv run pytest -m "not integration"

test-integration:
	uv run pytest -m integration
