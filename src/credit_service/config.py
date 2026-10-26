from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
    mongo_db: str = "credit_service"
    mongo_timeout_ms: int = 5000

    broker_url: str = "amqp://guest:guest@localhost:5672//"
    credits_queue: str = "credits"

    # Celery retries for transient MongoDB errors, with exponential backoff + full jitter.
    task_max_retries: int = 5
    retry_backoff_seconds: float = 1.0
    retry_backoff_max_seconds: float = 60.0
    # Hard cap across retries, redeliveries and requeues before a credit is marked failed.
    max_attempts: int = 10

    # Sweeper: republish credits stuck in `processing` (publish failed, message lost).
    # stale_after must comfortably exceed queue latency plus the retry backoff window.
    sweep_interval_seconds: float = 30.0
    stale_after_seconds: float = 120.0
    sweep_batch_size: int = 100

    log_level: str = "INFO"
    log_format: str = "json"  # or "console" for local development


@lru_cache
def get_settings() -> Settings:
    return Settings()
