from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA_PATH = Path(__file__).with_name("schema.sql")
# Odbudowa schematu jest celowo destrukcyjna: importer używa jej po to,
# aby każdy nowy import CSV trafiał do aktualnej wersji schematu aplikacji.
RESET_SCHEMA_SQL = """
PRAGMA foreign_keys = OFF;
DROP VIEW IF EXISTS patient_overview;
DROP VIEW IF EXISTS patient_summary;
DROP VIEW IF EXISTS diagnosis_statistics;
DROP VIEW IF EXISTS data_quality_report;
DROP TRIGGER IF EXISTS trg_care_unit_stays_validate_visit_number;
DROP TABLE IF EXISTS care_unit_stays;
DROP TABLE IF EXISTS admissions;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS import_rows;
PRAGMA foreign_keys = ON;
"""


@contextmanager
def get_connection(db_path: str | Path):
    """Otwiera krótkotrwałe połączenie SQLite skonfigurowane dla tej aplikacji."""
    target = str(db_path)
    connection = sqlite3.connect(
        target,
        uri=target.startswith("file:"),
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000;")
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA journal_mode = DELETE;")
    try:
        yield connection
    finally:
        connection.close()


def initialize_schema(
    connection: sqlite3.Connection,
    schema_path: str | Path | None = None,
) -> None:
    """Tworzy obiekty schematu, jeśli nie istnieją jeszcze w bazie."""
    resolved_schema = Path(schema_path) if schema_path is not None else SCHEMA_PATH
    connection.executescript(resolved_schema.read_text(encoding="utf-8"))


def rebuild_schema(
    connection: sqlite3.Connection,
    schema_path: str | Path | None = None,
) -> None:
    """Usuwa bieżący schemat i odtwarza go z głównego pliku SQL."""
    resolved_schema = Path(schema_path) if schema_path is not None else SCHEMA_PATH
    connection.executescript(RESET_SCHEMA_SQL)
    connection.executescript(resolved_schema.read_text(encoding="utf-8"))
