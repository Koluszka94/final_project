from __future__ import annotations

"""Czytelne etykiety używane w tabelach, filtrach i wynikach grupowania."""


COLUMN_LABELS = {
    "patient_id": "Identyfikator pacjenta",
    "admission_id": "Identyfikator hospitalizacji",
    "care_unit_stay_id": "Identyfikator pobytu oddziałowego",
    "gender": "Płeć",
    "ethnicity": "Pochodzenie etniczne",
    "age": "Wiek",
    "age_group": "Grupa wiekowa",
    "hospital_id": "Id szpitala",
    "ward_id": "Kod oddziału",
    "unit_type": "Typ oddziału",
    "unit_stay_type": "Typ pobytu oddziałowego",
    "diagnosis": "Rozpoznanie",
    "hospital_discharge_status": "Status wypisu ze szpitala",
    "unit_discharge_status": "Status wypisu z oddziału",
    "admission_height": "Wzrost przy przyjęciu",
    "admission_weight": "Masa przy przyjęciu",
    "discharge_weight": "Masa przy wypisie",
    "weight_change_kg": "Zmiana masy [kg]",
    "hospital_stay_hours": "Czas hospitalizacji [h]",
    "unit_stay_hours": "Czas pobytu oddziałowego [h]",
    "record_count": "Liczba rekordów",
    "patient_count": "Liczba pacjentów",
    "admission_count": "Liczba hospitalizacji",
    "care_unit_stay_count": "Liczba pobytów oddziałowych",
    "first_discharge_year": "Pierwszy rok wypisu",
    "last_discharge_year": "Ostatni rok wypisu",
    "expired_care_unit_stay_count": "Liczba zgonów oddziałowych",
    "avg_age": "Średni wiek",
    "avg_admission_weight": "Średnia masa przy przyjęciu",
    "avg_discharge_weight": "Średnia masa przy wypisie",
    "avg_hospital_stay_hours": "Średni czas hospitalizacji [h]",
    "avg_unit_stay_hours": "Średni czas pobytu oddziałowego [h]",
    "avg_weight_change_kg": "Średnia zmiana masy [kg]",
    "unit_death_count": "Liczba zgonów oddziałowych",
    "unit_mortality_pct": "Śmiertelność oddziałowa [%]",
    "source_name": "Źródło danych",
    "column_name": "Kolumna",
    "missing_count": "Liczba braków",
    "group_value": "Wartość grupy",
    "related_rows": "Liczba powiązanych rekordów",
    "metric_value": "Wartość miary",
}

VALUE_LABELS = {
    "gender": {
        "Female": "Kobieta",
        "Male": "Mężczyzna",
        "Unknown": "Nieznana",
    },
    "hospital_discharge_status": {
        "Alive": "Żywy",
        "Expired": "Zgon",
    },
    "unit_discharge_status": {
        "Alive": "Żywy",
        "Expired": "Zgon",
    },
    "age_group": {
        "0-17": "0-17 lat",
        "18-34": "18-34 lata",
        "35-49": "35-49 lat",
        "50-64": "50-64 lata",
        "65-79": "65-79 lat",
        "80-89": "80-89 lat",
        "(missing)": "(brak danych)",
    },
    "unit_type": {
        "CCU-CTICU": "Połączony oddział intensywnej terapii kardiologicznej i kardiochirurgicznej (CCU-CTICU)",
        "CSICU": "Oddział intensywnej terapii kardiochirurgicznej (CSICU)",
        "CTICU": "Oddział intensywnej terapii kardiotorakochirurgicznej (CTICU)",
        "Cardiac ICU": "Kardiologiczny oddział intensywnej terapii",
        "Cardiac Surgery": "Oddział kardiochirurgii",
        "MICU": "Internistyczny oddział intensywnej terapii (MICU)",
        "Med-Surg ICU": "Internistyczno-chirurgiczny oddział intensywnej terapii",
        "Neuro ICU": "Neurologiczny oddział intensywnej terapii",
        "SICU": "Chirurgiczny oddział intensywnej terapii (SICU)",
    },
    "unit_stay_type": {
        "admit": "Przyjęcie na oddział",
        "readmit": "Ponowne przyjęcie na oddział",
        "stepdown/other": "Pobyt po przeniesieniu typu step-down lub inny",
        "transfer": "Transfer lub przeniesienie",
    },
    "group_value": {
        "(missing)": "(brak danych)",
    },
}


def get_column_label(column_name: str) -> str:
    """Zwraca polską etykietę dla technicznej nazwy kolumny DataFrame."""
    return COLUMN_LABELS.get(column_name, column_name.replace("_", " ").capitalize())


def get_value_label(field_name: str, value: object) -> str:
    """Tłumaczy wybrane wartości kodowane na polskie etykiety dla użytkownika."""
    if value is None:
        return ""

    text = str(value)
    if text == "(missing)":
        return "(brak danych)"

    return VALUE_LABELS.get(field_name, {}).get(text, text)
