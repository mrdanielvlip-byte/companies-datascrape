from enum import Enum
from typing import Optional
from pydantic_settings import BaseSettings
from pathlib import Path


class EnrichmentScopeEnum(str, Enum):
    TARGETED = "targeted"
    DELTA = "delta"
    FULL = "full"


class Settings(BaseSettings):
    database_url: str
    redis_url: str
    ch_api_key: str
    enrichment_scope: EnrichmentScopeEnum = EnrichmentScopeEnum.TARGETED
    max_workers: int = 4
    api_rate_limit_per_minute: int = 400
    bulk_data_dir: Path = Path("./data/raw")
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


def get_settings() -> Settings:
    """Load settings from environment."""
    return Settings()
