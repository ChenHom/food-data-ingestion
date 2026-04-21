from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from food_data_ingestion.config import Settings
from food_data_ingestion.db.connection import build_dsn, create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession


# ---------------------------------------------------------------------------
# build_dsn
# ---------------------------------------------------------------------------


def test_build_dsn_contains_all_fields():
    settings = Settings(
        db_host="myhost",
        db_port=5433,
        db_name="mydb",
        db_user="myuser",
        db_password="mypassword",
    )
    dsn = build_dsn(settings)

    assert "host=myhost" in dsn
    assert "port=5433" in dsn
    assert "dbname=mydb" in dsn
    assert "user=myuser" in dsn
    assert "password=mypassword" in dsn


def test_build_dsn_does_not_omit_password():
    settings = Settings(db_password="s3cr3t")
    dsn = build_dsn(settings)
    assert "s3cr3t" in dsn


# ---------------------------------------------------------------------------
# create_connection
# ---------------------------------------------------------------------------


def test_create_connection_calls_psycopg_connect():
    settings = Settings(
        db_host="testhost",
        db_port=5432,
        db_name="testdb",
        db_user="testuser",
        db_password="testpass",
    )
    mock_conn = MagicMock()
    with patch("food_data_ingestion.db.connection.psycopg.connect", return_value=mock_conn) as mock_connect:
        conn = create_connection(settings)

    mock_connect.assert_called_once_with(build_dsn(settings))
    assert conn is mock_conn


def test_create_connection_raises_connection_error_on_failure():
    import psycopg as _psycopg

    settings = Settings(db_host="badhost", db_name="baddb")
    with patch(
        "food_data_ingestion.db.connection.psycopg.connect",
        side_effect=_psycopg.OperationalError("connection refused"),
    ):
        with pytest.raises(ConnectionError) as exc_info:
            create_connection(settings)

    assert "badhost" in str(exc_info.value)
    assert "baddb" in str(exc_info.value)


def test_create_connection_error_message_does_not_include_password():
    import psycopg as _psycopg

    settings = Settings(db_host="myhost", db_name="mydb", db_password="supersecret")
    with patch(
        "food_data_ingestion.db.connection.psycopg.connect",
        side_effect=_psycopg.OperationalError("connection refused"),
    ):
        with pytest.raises(ConnectionError) as exc_info:
            create_connection(settings)

    assert "supersecret" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# PsycopgSession — helpers to build a mock connection
# ---------------------------------------------------------------------------


def _make_cursor(rows=None, single_row=None):
    """Return a mock cursor context manager that yields a mock cursor."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = single_row
    mock_cursor.fetchall.return_value = rows or []
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=mock_cursor)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, mock_cursor


def _make_conn(rows=None, single_row=None):
    ctx, cursor = _make_cursor(rows=rows, single_row=single_row)
    conn = MagicMock()
    conn.cursor.return_value = ctx
    return conn, cursor


# ---------------------------------------------------------------------------
# fetchone
# ---------------------------------------------------------------------------


def test_fetchone_returns_dict_row():
    expected = {"id": 1, "name": "拉麵店"}
    conn, cursor = _make_conn(single_row=expected)
    session = PsycopgSession(conn)

    result = session.fetchone("SELECT * FROM restaurants WHERE id = %s", (1,))

    cursor.execute.assert_called_once_with("SELECT * FROM restaurants WHERE id = %s", (1,))
    assert result == expected


def test_fetchone_returns_none_when_no_row():
    conn, cursor = _make_conn(single_row=None)
    session = PsycopgSession(conn)

    result = session.fetchone("SELECT * FROM restaurants WHERE id = %s", (999,))

    assert result is None


# ---------------------------------------------------------------------------
# fetchall
# ---------------------------------------------------------------------------


def test_fetchall_returns_list_of_dicts():
    rows = [{"id": 1, "name": "拉麵店"}, {"id": 2, "name": "壽司店"}]
    conn, cursor = _make_conn(rows=rows)
    session = PsycopgSession(conn)

    result = session.fetchall("SELECT * FROM restaurants", ())

    cursor.execute.assert_called_once()
    assert result == rows


def test_fetchall_returns_empty_list_when_no_rows():
    conn, cursor = _make_conn(rows=[])
    session = PsycopgSession(conn)

    result = session.fetchall("SELECT * FROM restaurants WHERE id = %s", (0,))

    assert result == []


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------


def test_execute_calls_cursor_execute():
    conn, cursor = _make_conn()
    session = PsycopgSession(conn)

    session.execute("UPDATE restaurants SET name = %s WHERE id = %s", ("新名稱", 1))

    cursor.execute.assert_called_once_with(
        "UPDATE restaurants SET name = %s WHERE id = %s", ("新名稱", 1)
    )


def test_execute_does_not_return_value():
    conn, _ = _make_conn()
    session = PsycopgSession(conn)

    result = session.execute("DELETE FROM restaurants WHERE id = %s", (1,))

    assert result is None


# ---------------------------------------------------------------------------
# execute_returning
# ---------------------------------------------------------------------------


def test_execute_returning_returns_dict_row():
    expected = {"id": 42, "name": "壽司店"}
    conn, cursor = _make_conn(single_row=expected)
    session = PsycopgSession(conn)

    result = session.execute_returning(
        "INSERT INTO restaurants (name) VALUES (%s) RETURNING id, name", ("壽司店",)
    )

    cursor.execute.assert_called_once()
    assert result == expected


def test_execute_returning_returns_none_when_no_row():
    conn, _ = _make_conn(single_row=None)
    session = PsycopgSession(conn)

    result = session.execute_returning(
        "INSERT INTO restaurants (name) VALUES (%s) RETURNING id", ("壽司店",)
    )

    assert result is None


# ---------------------------------------------------------------------------
# SessionProtocol compatibility
# ---------------------------------------------------------------------------


def test_psycopg_session_is_compatible_with_session_protocol():
    """PsycopgSession must satisfy the SessionProtocol used by repositories."""
    from food_data_ingestion.storage.cache_repository import SessionProtocol
    from typing import runtime_checkable, Protocol

    # Verify structural compatibility by checking the required methods exist
    session = PsycopgSession(MagicMock())
    assert hasattr(session, "fetchone")
    assert hasattr(session, "execute")
    assert callable(session.fetchone)
    assert callable(session.execute)
