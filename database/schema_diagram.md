# Wizualizacja schematu bazy danych

Ponizsze diagramy opisuja aktualny schemat z `database/schema.sql` i odpowiadaja obiektom znalezionym w `ehr_app.db`.

## ERD tabel

```mermaid
erDiagram
    patients ||--o{ admissions : patient_id
    admissions ||--o{ care_unit_stays : admission_id

    import_rows {
        INTEGER import_row_id PK
        INTEGER care_unit_stay_id
        INTEGER admission_id
        TEXT patient_id
        TEXT gender
        INTEGER age
        TEXT ethnicity
        INTEGER hospital_id
        INTEGER ward_id
        TEXT diagnosis
        REAL admission_height
        REAL admission_weight
        REAL discharge_weight
        REAL weight_change_kg
        TEXT unit_type
        TEXT unit_discharge_status
    }

    patients {
        TEXT patient_id PK
        TEXT gender
        TEXT ethnicity
    }

    admissions {
        INTEGER admission_id PK
        TEXT patient_id FK
        INTEGER age
        INTEGER hospital_id
        REAL admission_height
        TEXT hospital_admit_time
        TEXT hospital_admit_source
        INTEGER discharge_year
        TEXT hospital_discharge_time
        TEXT hospital_discharge_location
        TEXT hospital_discharge_status
        INTEGER related_rows
    }

    care_unit_stays {
        INTEGER care_unit_stay_id PK
        INTEGER admission_id FK
        INTEGER ward_id
        TEXT diagnosis
        INTEGER hospital_admit_offset
        INTEGER hospital_discharge_offset
        TEXT unit_type
        TEXT unit_admit_time
        TEXT unit_admit_source
        INTEGER unit_visit_number
        TEXT unit_stay_type
        REAL admission_weight
        REAL discharge_weight
        REAL weight_change_kg
        TEXT unit_discharge_time
        INTEGER unit_discharge_offset
        TEXT unit_discharge_location
        TEXT unit_discharge_status
    }
```

## Widoki i logika bazy

```mermaid
flowchart LR
    import_rows[(import_rows)]
    patients[(patients)]
    admissions[(admissions)]
    care_unit_stays[(care_unit_stays)]

    patient_overview[[patient_overview]]
    patient_summary[[patient_summary]]
    diagnosis_statistics[[diagnosis_statistics]]
    data_quality_report[[data_quality_report]]
    trg_validate_visit{{trg_care_unit_stays_validate_visit_number}}

    import_rows -. zrodlo importu .-> patients
    import_rows -. zrodlo importu .-> admissions
    import_rows -. zrodlo importu .-> care_unit_stays

    patients --> admissions
    admissions --> care_unit_stays

    patients --> patient_overview
    admissions --> patient_overview
    care_unit_stays --> patient_overview

    patients --> patient_summary
    admissions --> patient_summary
    care_unit_stays --> patient_summary

    patient_overview --> diagnosis_statistics
    import_rows --> data_quality_report
    trg_validate_visit -. waliduje INSERT .-> care_unit_stays
```

## Najwazniejsze relacje

- `patients (1) -> (N) admissions`
- `admissions (1) -> (N) care_unit_stays`
- `import_rows` jest tabela stagingowa do importu CSV i nie ma zdefiniowanych kluczy obcych
- `patient_overview` laczy `patients`, `admissions` i `care_unit_stays` przez `INNER JOIN`
- `patient_summary` agreguje dane na poziomie pacjenta przez `LEFT JOIN`
- `diagnosis_statistics` liczy statystyki na podstawie `patient_overview`
- `data_quality_report` raportuje braki danych w `import_rows`
- trigger `trg_care_unit_stays_validate_visit_number` blokuje zapis, gdy `unit_visit_number < 1`

## Uwagi

- Diagram ERD pokazuje glowne kolumny i klucze. Pelna definicja wszystkich pol jest w `database/schema.sql`.
- Linie przerywane oznaczaja zaleznosci logiczne lub przeplyw danych, a nie klucze obce wymuszane przez SQLite.
