from __future__ import annotations

"""Budowanie zapytań SQL i pomocniki dla zakładki przeglądania rekordów."""

from dataclasses import dataclass

import pandas as pd


LOOKUP_FIELDS = {
    "gender": "gender",
    "ethnicity": "ethnicity",
    "unit_type": "unit_type",
    "unit_stay_type": "unit_stay_type",
    "hospital_discharge_status": "hospital_discharge_status",
    "unit_discharge_status": "unit_discharge_status",
    "age_group": "age_group",
}

RESULT_COLUMNS = [
    "care_unit_stay_id",
    "admission_id",
    "patient_id",
    "gender",
    "ethnicity",
    "age",
    "age_group",
    "hospital_id",
    "ward_id",
    "unit_type",
    "unit_stay_type",
    "diagnosis",
    "hospital_discharge_status",
    "unit_discharge_status",
    "admission_weight",
    "discharge_weight",
    "weight_change_kg",
    "hospital_stay_hours",
    "unit_stay_hours",
]


@dataclass(frozen=True)
class SearchFilters:
    """Znormalizowany zestaw filtrów wspólny dla wyszukiwania, analiz i wykresów."""

    patient_id: str | None = None
    admission_id: int | None = None
    care_unit_stay_id: int | None = None
    gender: str | None = None
    ethnicity: str | None = None
    age_min: int | None = None
    age_max: int | None = None
    age_group: str | None = None
    hospital_id: int | None = None
    ward_id: int | None = None
    unit_type: str | None = None
    unit_stay_type: str | None = None
    hospital_discharge_status: str | None = None
    unit_discharge_status: str | None = None
    diagnosis_contains: str | None = None
    limit: int = 500

    def normalized(self) -> "SearchFilters":
        """Zwraca oczyszczoną kopię bezpieczną do bindowania parametrów SQL."""
        return SearchFilters(
            patient_id=_clean_text(self.patient_id),
            admission_id=self.admission_id,
            care_unit_stay_id=self.care_unit_stay_id,
            gender=_clean_text(self.gender),
            ethnicity=_clean_text(self.ethnicity),
            age_min=self.age_min,
            age_max=self.age_max,
            age_group=_clean_text(self.age_group),
            hospital_id=self.hospital_id,
            ward_id=self.ward_id,
            unit_type=_clean_text(self.unit_type),
            unit_stay_type=_clean_text(self.unit_stay_type),
            hospital_discharge_status=_clean_text(self.hospital_discharge_status),
            unit_discharge_status=_clean_text(self.unit_discharge_status),
            diagnosis_contains=_clean_text(self.diagnosis_contains),
            limit=max(1, min(int(self.limit), 5000)),
        )


def fetch_unit_stays(connection, filters: SearchFilters) -> pd.DataFrame:
    """Wczytuje szczegółowe rekordy pobytów z analitycznego widoku pacjentów."""
    normalized = filters.normalized()
    where_clause, parameters = build_where_clause(normalized)
    query = f"""
        SELECT
            {", ".join(RESULT_COLUMNS)}
        FROM patient_overview
        {where_clause}
        ORDER BY
            COALESCE(discharge_year, 0) DESC,
            admission_id DESC,
            care_unit_stay_id DESC
        LIMIT ?
    """
    parameters.append(normalized.limit)
    return pd.read_sql_query(query, connection, params=parameters)


def fetch_lookup_values(connection, field_name: str) -> list[str]:
    """Wczytuje unikalne wartości do list rozwijanych z widoku analitycznego."""
    if field_name not in LOOKUP_FIELDS:
        raise ValueError(f"Unsupported lookup field: {field_name}")

    query = f"""
        SELECT DISTINCT {LOOKUP_FIELDS[field_name]} AS value
        FROM patient_overview
        WHERE {LOOKUP_FIELDS[field_name]} IS NOT NULL
          AND TRIM(CAST({LOOKUP_FIELDS[field_name]} AS TEXT)) <> ''
        ORDER BY CAST({LOOKUP_FIELDS[field_name]} AS TEXT)
    """
    rows = connection.execute(query).fetchall()
    return [str(row["value"]) for row in rows]


def fetch_overview_metrics(connection) -> dict[str, float | int]:
    """Wczytuje metryki podsumowania wyświetlane w zakładce importu."""
    query = """
        SELECT
            (SELECT COUNT(*) FROM patients) AS patient_count,
            (SELECT COUNT(*) FROM admissions) AS admission_count,
            (SELECT COUNT(*) FROM care_unit_stays) AS care_unit_stay_count,
            (
                SELECT ROUND(
                    100.0 * AVG(
                        CASE WHEN unit_discharge_status = 'Expired' THEN 1.0 ELSE 0.0 END
                    ),
                    2
                )
                FROM care_unit_stays
            ) AS unit_mortality_pct,
            (
                SELECT ROUND(
                    100.0 * AVG(
                        CASE
                            WHEN hospital_discharge_status = 'Expired' THEN 1.0
                            ELSE 0.0
                        END
                    ),
                    2
                )
                FROM admissions
            ) AS hospital_mortality_pct
    """
    row = connection.execute(query).fetchone()
    return {
        "patient_count": int(row["patient_count"] or 0),
        "admission_count": int(row["admission_count"] or 0),
        "care_unit_stay_count": int(row["care_unit_stay_count"] or 0),
        "unit_mortality_pct": float(row["unit_mortality_pct"] or 0.0),
        "hospital_mortality_pct": float(row["hospital_mortality_pct"] or 0.0),
    }


def build_where_clause(filters: SearchFilters) -> tuple[str, list]:
    """Zamienia SearchFilters na klauzulę WHERE i listę parametrów SQL."""
    normalized = filters.normalized()
    conditions: list[str] = []
    parameters: list[object] = []

    if normalized.patient_id:
        conditions.append("patient_id LIKE ?")
        parameters.append(f"%{normalized.patient_id}%")

    if normalized.admission_id is not None:
        conditions.append("admission_id = ?")
        parameters.append(normalized.admission_id)

    if normalized.care_unit_stay_id is not None:
        conditions.append("care_unit_stay_id = ?")
        parameters.append(normalized.care_unit_stay_id)

    if normalized.gender:
        conditions.append("gender = ?")
        parameters.append(normalized.gender)

    if normalized.ethnicity:
        conditions.append("ethnicity = ?")
        parameters.append(normalized.ethnicity)

    if normalized.age_min is not None:
        conditions.append("age >= ?")
        parameters.append(normalized.age_min)

    if normalized.age_max is not None:
        conditions.append("age <= ?")
        parameters.append(normalized.age_max)

    if normalized.age_group:
        conditions.append("age_group = ?")
        parameters.append(normalized.age_group)

    if normalized.hospital_id is not None:
        conditions.append("hospital_id = ?")
        parameters.append(normalized.hospital_id)

    if normalized.ward_id is not None:
        conditions.append("ward_id = ?")
        parameters.append(normalized.ward_id)

    if normalized.unit_type:
        conditions.append("unit_type = ?")
        parameters.append(normalized.unit_type)

    if normalized.unit_stay_type:
        conditions.append("unit_stay_type = ?")
        parameters.append(normalized.unit_stay_type)

    if normalized.hospital_discharge_status:
        conditions.append("hospital_discharge_status = ?")
        parameters.append(normalized.hospital_discharge_status)

    if normalized.unit_discharge_status:
        conditions.append("unit_discharge_status = ?")
        parameters.append(normalized.unit_discharge_status)

    if normalized.diagnosis_contains:
        conditions.append("LOWER(COALESCE(diagnosis, '')) LIKE ?")
        parameters.append(f"%{normalized.diagnosis_contains.lower()}%")

    if not conditions:
        return "", parameters

    return "WHERE " + " AND ".join(conditions), parameters


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text == "(Any)":
        return None

    return text
