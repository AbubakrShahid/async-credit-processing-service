from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_uri: str = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
    mongo_db: str = "credit_service"
    mongo_timeout_ms: int = 5000

    broker_url: str = "amqp://guest:guest@localhost:5672//"
    credits_queue: str = "credits"

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
