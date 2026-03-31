from __future__ import annotations

"""Zapytania agregujące używane w zakładkach analiz i wykresów."""

import pandas as pd

from queries.patient_queries import SearchFilters, build_where_clause


GROUP_BY_FIELDS = {
    "gender": "gender",
    "ethnicity": "ethnicity",
    "hospital_id": "hospital_id",
    "ward_id": "ward_id",
    "unit_type": "unit_type",
    "unit_stay_type": "unit_stay_type",
    "hospital_discharge_status": "hospital_discharge_status",
    "unit_discharge_status": "unit_discharge_status",
    "age_group": "age_group",
}

HISTOGRAM_METRICS = {
    "age": "Wiek",
    "admission_height": "Wzrost przy przyjęciu",
    "admission_weight": "Masa przy przyjęciu",
    "discharge_weight": "Masa przy wypisie",
    "hospital_stay_hours": "Czas hospitalizacji [h]",
    "unit_stay_hours": "Czas pobytu oddziałowego [h]",
    "weight_change_kg": "Zmiana masy [kg]",
}

BAR_METRICS = {
    "record_count": "Liczba rekordów",
    "patient_count": "Liczba pacjentów",
    "avg_age": "Średni wiek",
    "avg_admission_weight": "Średnia masa przy przyjęciu",
    "avg_discharge_weight": "Średnia masa przy wypisie",
    "avg_hospital_stay_hours": "Średni czas hospitalizacji [h]",
    "avg_unit_stay_hours": "Średni czas pobytu oddziałowego [h]",
    "avg_weight_change_kg": "Średnia zmiana masy [kg]",
    "unit_mortality_pct": "Śmiertelność oddziałowa [%]",
}

BAR_METRIC_SQL = {
    "record_count": "COUNT(*)",
    "patient_count": "COUNT(DISTINCT patient_id)",
    "avg_age": "ROUND(AVG(age), 2)",
    "avg_admission_weight": "ROUND(AVG(admission_weight), 2)",
    "avg_discharge_weight": "ROUND(AVG(discharge_weight), 2)",
    "avg_hospital_stay_hours": "ROUND(AVG(hospital_stay_hours), 2)",
    "avg_unit_stay_hours": "ROUND(AVG(unit_stay_hours), 2)",
    "avg_weight_change_kg": "ROUND(AVG(weight_change_kg), 2)",
    "unit_mortality_pct": (
        "ROUND(100.0 * AVG("
        "CASE WHEN unit_discharge_status = 'Expired' THEN 1.0 ELSE 0.0 END"
        "), 2)"
    ),
}


def fetch_group_comparison(
    connection,
    group_by: str,
    filters: SearchFilters,
) -> pd.DataFrame:
    """Zwraca zestaw metryk zagregowanych dla wybranego pola grupowania."""
    if group_by not in GROUP_BY_FIELDS:
        raise ValueError(f"Unsupported group_by: {group_by}")

    group_expression = f"COALESCE(CAST({GROUP_BY_FIELDS[group_by]} AS TEXT), '(missing)')"
    where_clause, parameters = build_where_clause(filters)

    query = f"""
        SELECT
            {group_expression} AS group_value,
            COUNT(*) AS record_count,
            COUNT(DISTINCT patient_id) AS patient_count,
            ROUND(AVG(age), 2) AS avg_age,
            ROUND(AVG(admission_weight), 2) AS avg_admission_weight,
            ROUND(AVG(discharge_weight), 2) AS avg_discharge_weight,
            ROUND(AVG(hospital_stay_hours), 2) AS avg_hospital_stay_hours,
            ROUND(AVG(unit_stay_hours), 2) AS avg_unit_stay_hours,
            ROUND(AVG(weight_change_kg), 2) AS avg_weight_change_kg,
            SUM(CASE WHEN unit_discharge_status = 'Expired' THEN 1 ELSE 0 END)
                AS unit_death_count,
            ROUND(
                100.0 * AVG(
                    CASE WHEN unit_discharge_status = 'Expired' THEN 1.0 ELSE 0.0 END
                ),
                2
            ) AS unit_mortality_pct
        FROM patient_overview
        {where_clause}
        GROUP BY {group_expression}
        ORDER BY record_count DESC, group_value ASC
    """
    return pd.read_sql_query(query, connection, params=parameters)


def fetch_diagnosis_summary(
    connection,
    filters: SearchFilters,
    limit: int = 20,
) -> pd.DataFrame:
    """Zwraca najczęstsze rozpoznania dla aktualnie ustawionych filtrów."""
    where_clause, parameters = build_where_clause(filters)
    query = f"""
        SELECT
            COALESCE(diagnosis, '(missing)') AS diagnosis,
            COUNT(*) AS record_count,
            COUNT(DISTINCT patient_id) AS patient_count,
            COUNT(DISTINCT admission_id) AS admission_count,
            ROUND(AVG(unit_stay_hours), 2) AS avg_unit_stay_hours,
            ROUND(AVG(hospital_stay_hours), 2) AS avg_hospital_stay_hours,
            ROUND(
                100.0 * AVG(
                    CASE WHEN unit_discharge_status = 'Expired' THEN 1.0 ELSE 0.0 END
                ),
                2
            ) AS unit_mortality_pct
        FROM patient_overview
        {where_clause}
        GROUP BY COALESCE(diagnosis, '(missing)')
        ORDER BY record_count DESC, diagnosis ASC
        LIMIT ?
    """
    parameters.append(max(1, min(int(limit), 100)))
    return pd.read_sql_query(query, connection, params=parameters)


def fetch_histogram_series(
    connection,
    metric: str,
    filters: SearchFilters,
) -> pd.DataFrame:
    """Zwraca serię jednej miary gotową do narysowania histogramu."""
    if metric not in HISTOGRAM_METRICS:
        raise ValueError(f"Unsupported histogram metric: {metric}")

    where_clause, parameters = build_where_clause(filters)
    metric_condition = f"{metric} IS NOT NULL"
    if where_clause:
        where_clause = f"{where_clause} AND {metric_condition}"
    else:
        where_clause = f"WHERE {metric_condition}"

    query = f"""
        SELECT {metric} AS metric_value
        FROM patient_overview
        {where_clause}
        ORDER BY metric_value
    """
    return pd.read_sql_query(query, connection, params=parameters)


def fetch_group_metric_bar(
    connection,
    group_by: str,
    metric: str,
    filters: SearchFilters,
    limit: int = 15,
) -> pd.DataFrame:
    """Zwraca jedną zagregowaną miarę na grupę dla wykresu słupkowego."""
    if group_by not in GROUP_BY_FIELDS:
        raise ValueError(f"Unsupported group_by: {group_by}")
    if metric not in BAR_METRIC_SQL:
        raise ValueError(f"Unsupported bar metric: {metric}")

    group_expression = f"COALESCE(CAST({GROUP_BY_FIELDS[group_by]} AS TEXT), '(missing)')"
    metric_sql = BAR_METRIC_SQL[metric]
    where_clause, parameters = build_where_clause(filters)

    query = f"""
        SELECT
            {group_expression} AS group_value,
            {metric_sql} AS metric_value
        FROM patient_overview
        {where_clause}
        GROUP BY {group_expression}
        ORDER BY metric_value DESC, group_value ASC
        LIMIT ?
    """
    parameters.append(max(1, min(int(limit), 50)))
    return pd.read_sql_query(query, connection, params=parameters)
