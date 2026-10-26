.PHONY: up down logs ps lint fmt typecheck test test-unit test-integration

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest

test-unit:
	uv run pytest -m "not integration"

test-integration:
	uv run pytest -m integration
