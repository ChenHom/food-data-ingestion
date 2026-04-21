from __future__ import annotations

import psycopg

from food_data_ingestion.config import Settings


def build_dsn(settings: Settings) -> str:
    return (
        f"host={settings.db_host} "
        f"port={settings.db_port} "
        f"dbname={settings.db_name} "
        f"user={settings.db_user} "
        f"password={settings.db_password}"
    )


def create_connection(settings: Settings) -> psycopg.Connection:
    try:
        return psycopg.connect(build_dsn(settings))
    except psycopg.OperationalError as exc:
        raise ConnectionError(
            f"Failed to connect to PostgreSQL at {settings.db_host}:{settings.db_port} (db={settings.db_name})"
        ) from exc
