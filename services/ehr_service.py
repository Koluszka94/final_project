from __future__ import annotations

"""Warstwa serwisowa używana przez GUI do centralizacji dostępu do bazy."""

import sqlite3
from pathlib import Path

import pandas as pd

from analytics.group_comparison import (
    BAR_METRICS,
    GROUP_BY_FIELDS,
    HISTOGRAM_METRICS,
    fetch_diagnosis_summary,
    fetch_group_comparison,
    fetch_group_metric_bar,
    fetch_histogram_series,
)
from database.connection import get_connection
from database.importer import ImportStats, import_csv_to_database
from queries.patient_queries import (
    LOOKUP_FIELDS,
    SearchFilters,
    fetch_lookup_values,
    fetch_overview_metrics,
    fetch_unit_stays,
)
from queries.view_queries import (
    fetch_data_quality_summary_view,
    fetch_diagnosis_summary_view,
    fetch_patient_summary_view,
)


DEFAULT_CSV_PATH = Path("EHR.csv")
DEFAULT_DB_PATH = Path("ehr_app.db")

GROUP_BY_OPTIONS = [
    ("gender", "Płeć"),
    ("ethnicity", "Pochodzenie etniczne"),
    ("hospital_id", "Szpital"),
    ("ward_id", "Oddział"),
    ("unit_type", "Typ oddziału"),
    ("unit_stay_type", "Typ pobytu oddziałowego"),
    ("hospital_discharge_status", "Status wypisu ze szpitala"),
    ("unit_discharge_status", "Status wypisu z oddziału"),
    ("age_group", "Grupa wiekowa"),
]

HISTOGRAM_OPTIONS = list(HISTOGRAM_METRICS.items())
GROUP_METRIC_OPTIONS = list(BAR_METRICS.items())
VIEW_OPTIONS = [
    ("patient_summary", "Podsumowanie pacjentów"),
    ("diagnosis_summary", "Statystyki rozpoznań"),
    ("data_quality_summary", "Raport jakości danych"),
]
LOOKUP_OPTION_FIELDS = [
    "gender",
    "ethnicity",
    "unit_type",
    "unit_stay_type",
    "hospital_discharge_status",
    "unit_discharge_status",
    "age_group",
]
REQUIRED_SCHEMA_OBJECTS = {
    "patients": "table",
    "admissions": "table",
    "care_unit_stays": "table",
    "patient_overview": "view",
}
REQUIRED_TABLE_COLUMNS = {
    "patients": {"patient_id"},
    "admissions": {"admission_id", "patient_id", "age"},
    "care_unit_stays": {"care_unit_stay_id", "admission_id", "diagnosis"},
}


class EHRService:
    """Cienka fasada nad importem, zapytaniami SQL i widokami analitycznymi."""

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH):
        self._db_path = Path(db_path)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def set_db_path(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)

    def database_exists(self) -> bool:
        db_text = str(self._db_path)
        return db_text.startswith("file:") or self._db_path.is_file()

    def database_ready(self) -> bool:
        """Sprawdza, czy bieżący plik bazy odpowiada oczekiwanemu schematowi."""
        if not self.database_exists():
            return False

        try:
            with get_connection(self._db_path) as connection:
                placeholders = ", ".join("?" for _ in REQUIRED_SCHEMA_OBJECTS)
                rows = connection.execute(
                    f"""
                    SELECT name, type
                    FROM sqlite_master
                    WHERE name IN ({placeholders})
                    """,
                    list(REQUIRED_SCHEMA_OBJECTS.keys()),
                ).fetchall()

                found = {row["name"]: row["type"] for row in rows}
                if not all(
                    found.get(name) == object_type
                    for name, object_type in REQUIRED_SCHEMA_OBJECTS.items()
                ):
                    return False

                for table_name, required_columns in REQUIRED_TABLE_COLUMNS.items():
                    column_rows = connection.execute(
                        f"PRAGMA table_info({table_name})"
                    ).fetchall()
                    available_columns = {row["name"] for row in column_rows}
                    if not required_columns.issubset(available_columns):
                        return False

                connection.execute(
                    """
                    SELECT
                        patient_id,
                        admission_id,
                        care_unit_stay_id
                    FROM patient_overview
                    LIMIT 1
                    """
                ).fetchall()
        except sqlite3.Error:
            return False

        return True

    def import_csv(self, csv_path: str | Path) -> ImportStats:
        return import_csv_to_database(csv_path, self._db_path)

    def load_lookup_options(self) -> dict[str, list[str]]:
        with get_connection(self._db_path) as connection:
            return {
                field_name: fetch_lookup_values(connection, field_name)
                for field_name in LOOKUP_OPTION_FIELDS
            }

    def load_overview_metrics(self) -> dict[str, float | int]:
        with get_connection(self._db_path) as connection:
            return fetch_overview_metrics(connection)

    def search_records(self, filters: SearchFilters) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            return fetch_unit_stays(connection, filters)

    def load_group_summary(
        self,
        group_by: str,
        filters: SearchFilters,
    ) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            return fetch_group_comparison(connection, group_by, filters)

    def load_diagnosis_summary(
        self,
        filters: SearchFilters,
        limit: int = 20,
    ) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            return fetch_diagnosis_summary(connection, filters, limit=limit)

    def load_histogram_data(
        self,
        metric: str,
        filters: SearchFilters,
    ) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            return fetch_histogram_series(connection, metric, filters)

    def load_group_chart_data(
        self,
        group_by: str,
        metric: str,
        filters: SearchFilters,
        limit: int = 15,
    ) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            return fetch_group_metric_bar(
                connection,
                group_by,
                metric,
                filters,
                limit=limit,
            )

    def load_view_data(
        self,
        view_name: str,
        limit: int = 500,
    ) -> pd.DataFrame:
        with get_connection(self._db_path) as connection:
            if view_name == "patient_summary":
                return fetch_patient_summary_view(connection, limit=limit)
            if view_name == "diagnosis_summary":
                return fetch_diagnosis_summary_view(connection, limit=limit)
            if view_name == "data_quality_summary":
                return fetch_data_quality_summary_view(connection)

        raise ValueError(f"Unsupported view: {view_name}")


__all__ = [
    "BAR_METRICS",
    "DEFAULT_CSV_PATH",
    "DEFAULT_DB_PATH",
    "EHRService",
    "GROUP_BY_FIELDS",
    "GROUP_BY_OPTIONS",
    "GROUP_METRIC_OPTIONS",
    "HISTOGRAM_METRICS",
    "HISTOGRAM_OPTIONS",
    "ImportStats",
    "LOOKUP_FIELDS",
    "LOOKUP_OPTION_FIELDS",
    "SearchFilters",
    "VIEW_OPTIONS",
]
