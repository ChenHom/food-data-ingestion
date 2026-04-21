from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "food_ingestion"
    db_user: str = "researcher"
    db_password: str = "research_pass"
    google_places_api_key: str = ""
    default_cache_ttl_seconds: int = 21600
    default_error_cache_ttl_seconds: int = 60
    app_env: str = "development"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_host=os.getenv("DB_HOST", cls.db_host),
            db_port=int(os.getenv("DB_PORT", str(cls.db_port))),
            db_name=os.getenv("DB_NAME", cls.db_name),
            db_user=os.getenv("DB_USER", cls.db_user),
            db_password=os.getenv("DB_PASSWORD", cls.db_password),
            google_places_api_key=os.getenv("GOOGLE_PLACES_API_KEY", cls.google_places_api_key),
            default_cache_ttl_seconds=int(
                os.getenv("DEFAULT_CACHE_TTL_SECONDS", str(cls.default_cache_ttl_seconds))
            ),
            default_error_cache_ttl_seconds=int(
                os.getenv("DEFAULT_ERROR_CACHE_TTL_SECONDS", str(cls.default_error_cache_ttl_seconds))
            ),
            app_env=os.getenv("APP_ENV", cls.app_env),
        )
