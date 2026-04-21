from __future__ import annotations

import os

from food_data_ingestion.config import Settings


def test_settings_use_project_defaults_when_env_missing(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    settings = Settings.from_env()

    assert settings.db_host == "localhost"
    assert settings.db_port == 5432
    assert settings.db_name == "food_ingestion"
    assert settings.db_user == "researcher"
    assert settings.db_password == "research_pass"


def test_settings_allow_env_override(monkeypatch):
    monkeypatch.setenv("DB_HOST", "postgres-db")
    monkeypatch.setenv("DB_PORT", "6543")
    monkeypatch.setenv("DB_NAME", "food_ingestion_test")
    monkeypatch.setenv("DB_USER", "tester")
    monkeypatch.setenv("DB_PASSWORD", "secret")

    settings = Settings.from_env()

    assert settings.db_host == "postgres-db"
    assert settings.db_port == 6543
    assert settings.db_name == "food_ingestion_test"
    assert settings.db_user == "tester"
    assert settings.db_password == "secret"
