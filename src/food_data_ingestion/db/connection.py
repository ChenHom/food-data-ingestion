from __future__ import annotations

import psycopg
import psycopg.conninfo

from food_data_ingestion.config import Settings


def build_dsn(settings: Settings) -> str:
    return psycopg.conninfo.make_conninfo(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


def create_connection(settings: Settings) -> psycopg.Connection:
    try:
        return psycopg.connect(build_dsn(settings))
    except psycopg.OperationalError as exc:
        raise ConnectionError(
            f"Failed to connect to PostgreSQL at {settings.db_host}:{settings.db_port} (db={settings.db_name})"
        ) from exc
