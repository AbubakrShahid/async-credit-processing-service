from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
    mongo_db: str = "credit_service"
    mongo_timeout_ms: int = 5000

    broker_url: str = "amqp://guest:guest@localhost:5672//"
    credits_queue: str = "credits"

    # API keys as "client:key,client2:key2". Empty disables authentication (local dev only).
    api_keys: str = ""

    # Admission control, checked before a credit is accepted.
    max_pending_per_user: int = 1_000  # 429 beyond this many in-flight credits per user
    max_queue_depth: int = 100_000  # 503 while the broker backlog is this deep
    # After a failed publish the API stops trying for this long and leaves new credits to
    # the dispatcher, so a broker outage doesn't make every request wait on retries.
    publish_pause_seconds: float = 5.0

    # Worker: transient MongoDB errors are retried with exponential backoff + full jitter
    # and never fail a credit. Business errors fail it at once; executions that die or hit
    # an unexpected error (poison messages) fail it after `max_crashes`.
    task_max_retries: int = 5
    retry_backoff_seconds: float = 1.0
    retry_backoff_max_seconds: float = 60.0
    max_crashes: int = 3
    task_time_limit_seconds: int = 300

    # Dispatcher: publishes credits whose publish never got confirmed, and, only while the
    # queue is drained, republishes confirmed credits that look lost.
    dispatch_interval_seconds: float = 15.0
    dispatch_time_budget_seconds: float = 10.0
    dispatch_batch_size: int = 500
    dispatch_lease_seconds: float = 60.0
    publish_grace_seconds: float = 10.0
    republish_after_seconds: float = 900.0
    reconcile_max_queue_depth: int = 100

    log_level: str = "INFO"
    log_format: str = "json"  # or "console" for local development

    def api_key_clients(self) -> dict[str, str]:
        """Map each API key to the client it belongs to."""
        pairs = (entry.split(":", 1) for entry in self.api_keys.split(",") if entry.strip())
        return {key.strip(): client.strip() for client, key in pairs}


@lru_cache
def get_settings() -> Settings:
    return Settings()
