from __future__ import annotations

"""Pomocniki do wczytywania gotowych widoków SQL pokazywanych w GUI."""

import pandas as pd


VIEW_LIMITS = {
    "patient_summary": 5000,
    "diagnosis_summary": 500,
}


def fetch_patient_summary_view(connection, limit: int = 500) -> pd.DataFrame:
    """Wczytuje widok podsumowania na poziomie pacjenta."""
    query = """
        SELECT
            patient_id,
            gender,
            ethnicity,
            admission_count,
            care_unit_stay_count,
            first_discharge_year,
            last_discharge_year,
            expired_care_unit_stay_count
        FROM patient_summary
        ORDER BY care_unit_stay_count DESC, patient_id ASC
        LIMIT ?
    """
    return pd.read_sql_query(query, connection, params=[_normalized_limit(limit, "patient_summary")])


def fetch_diagnosis_summary_view(connection, limit: int = 100) -> pd.DataFrame:
    """Wczytuje widok statystyk rozpoznań."""
    query = """
        SELECT
            diagnosis,
            record_count,
            patient_count,
            admission_count,
            avg_unit_stay_hours,
            avg_hospital_stay_hours,
            unit_mortality_pct
        FROM diagnosis_statistics
        ORDER BY record_count DESC, diagnosis ASC
        LIMIT ?
    """
    return pd.read_sql_query(
        query,
        connection,
        params=[_normalized_limit(limit, "diagnosis_summary")],
    )


def fetch_data_quality_summary_view(connection) -> pd.DataFrame:
    """Wczytuje raport opisujący braki danych w zaimportowanym zbiorze."""
    query = """
        SELECT
            source_name,
            column_name,
            missing_count
        FROM data_quality_report
        ORDER BY missing_count DESC, column_name ASC
    """
    return pd.read_sql_query(query, connection)


def _normalized_limit(limit: int, view_name: str) -> int:
    """Ogranicza podany limit do bezpiecznego zakresu dla danego widoku SQL."""
    max_limit = VIEW_LIMITS[view_name]
    return max(1, min(int(limit), max_limit))
