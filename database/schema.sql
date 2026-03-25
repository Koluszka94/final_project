CREATE TABLE IF NOT EXISTS import_rows (
    import_row_id INTEGER PRIMARY KEY,
    care_unit_stay_id INTEGER NOT NULL,
    admission_id INTEGER NOT NULL,
    gender TEXT,
    age INTEGER,
    ethnicity TEXT,
    hospital_id INTEGER,
    ward_id INTEGER,
    diagnosis TEXT,
    admission_height REAL,
    hospital_admit_time TEXT,
    hospital_admit_offset INTEGER,
    hospital_admit_source TEXT,
    discharge_year INTEGER,
    hospital_discharge_time TEXT,
    hospital_discharge_offset INTEGER,
    hospital_discharge_location TEXT,
    hospital_discharge_status TEXT,
    unit_type TEXT,
    unit_admit_time TEXT,
    unit_admit_source TEXT,
    unit_visit_number INTEGER,
    unit_stay_type TEXT,
    admission_weight REAL,
    discharge_weight REAL,
    weight_change_kg REAL,
    unit_discharge_time TEXT,
    unit_discharge_offset INTEGER,
    unit_discharge_location TEXT,
    unit_discharge_status TEXT,
    patient_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS patients (
    patient_id TEXT PRIMARY KEY,
    gender TEXT,
    ethnicity TEXT
);

CREATE TABLE IF NOT EXISTS admissions (
    admission_id INTEGER PRIMARY KEY,
    patient_id TEXT NOT NULL,
    age INTEGER,
    hospital_id INTEGER,
    admission_height REAL,
    hospital_admit_time TEXT,
    hospital_admit_source TEXT,
    discharge_year INTEGER,
    hospital_discharge_time TEXT,
    hospital_discharge_location TEXT,
    hospital_discharge_status TEXT,
    related_rows INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (patient_id) REFERENCES patients (patient_id)
);

CREATE TABLE IF NOT EXISTS care_unit_stays (
    care_unit_stay_id INTEGER PRIMARY KEY,
    admission_id INTEGER NOT NULL,
    ward_id INTEGER,
    diagnosis TEXT,
    hospital_admit_offset INTEGER,
    hospital_discharge_offset INTEGER,
    unit_type TEXT,
    unit_admit_time TEXT,
    unit_admit_source TEXT,
    unit_visit_number INTEGER,
    unit_stay_type TEXT,
    admission_weight REAL,
    discharge_weight REAL,
    weight_change_kg REAL,
    unit_discharge_time TEXT,
    unit_discharge_offset INTEGER CHECK (
        unit_discharge_offset IS NULL OR unit_discharge_offset >= 0
    ),
    unit_discharge_location TEXT,
    unit_discharge_status TEXT,
    FOREIGN KEY (admission_id) REFERENCES admissions (admission_id)
);

CREATE INDEX IF NOT EXISTS idx_patients_gender
    ON patients (gender);
CREATE INDEX IF NOT EXISTS idx_patients_ethnicity
    ON patients (ethnicity);
CREATE INDEX IF NOT EXISTS idx_admissions_patient
    ON admissions (patient_id);
CREATE INDEX IF NOT EXISTS idx_admissions_hospital
    ON admissions (hospital_id);
CREATE INDEX IF NOT EXISTS idx_care_unit_stays_admission
    ON care_unit_stays (admission_id);
CREATE INDEX IF NOT EXISTS idx_care_unit_stays_ward
    ON care_unit_stays (ward_id);
CREATE INDEX IF NOT EXISTS idx_care_unit_stays_unit_type
    ON care_unit_stays (unit_type);
CREATE INDEX IF NOT EXISTS idx_care_unit_stays_unit_stay_type
    ON care_unit_stays (unit_stay_type);
CREATE INDEX IF NOT EXISTS idx_care_unit_stays_unit_status
    ON care_unit_stays (unit_discharge_status);

DROP VIEW IF EXISTS patient_overview;
CREATE VIEW patient_overview AS
SELECT
    cus.care_unit_stay_id,
    adm.admission_id,
    pat.patient_id,
    pat.gender,
    pat.ethnicity,
    adm.age,
    adm.hospital_id,
    adm.admission_height,
    adm.hospital_admit_time,
    adm.hospital_admit_source,
    adm.discharge_year,
    adm.hospital_discharge_time,
    adm.hospital_discharge_location,
    adm.hospital_discharge_status,
    cus.ward_id,
    cus.diagnosis,
    cus.hospital_admit_offset,
    cus.hospital_discharge_offset,
    cus.unit_type,
    cus.unit_admit_time,
    cus.unit_admit_source,
    cus.unit_visit_number,
    cus.unit_stay_type,
    cus.admission_weight,
    cus.discharge_weight,
    cus.weight_change_kg,
    cus.unit_discharge_time,
    cus.unit_discharge_offset,
    cus.unit_discharge_location,
    cus.unit_discharge_status,
    ROUND(
        CASE
            WHEN cus.hospital_discharge_offset IS NOT NULL
             AND cus.hospital_admit_offset IS NOT NULL
            THEN (cus.hospital_discharge_offset - cus.hospital_admit_offset) / 60.0
        END,
        2
    ) AS hospital_stay_hours,
    ROUND(
        CASE
            WHEN cus.unit_discharge_offset IS NOT NULL
            THEN cus.unit_discharge_offset / 60.0
        END,
        2
    ) AS unit_stay_hours,
    CASE
        WHEN adm.age BETWEEN 0 AND 17 THEN '0-17'
        WHEN adm.age BETWEEN 18 AND 34 THEN '18-34'
        WHEN adm.age BETWEEN 35 AND 49 THEN '35-49'
        WHEN adm.age BETWEEN 50 AND 64 THEN '50-64'
        WHEN adm.age BETWEEN 65 AND 79 THEN '65-79'
        WHEN adm.age BETWEEN 80 AND 89 THEN '80-89'
        ELSE '(missing)'
    END AS age_group
FROM care_unit_stays AS cus
JOIN admissions AS adm
    ON adm.admission_id = cus.admission_id
JOIN patients AS pat
    ON pat.patient_id = adm.patient_id;

DROP VIEW IF EXISTS patient_summary;
CREATE VIEW patient_summary AS
SELECT
    pat.patient_id,
    pat.gender,
    pat.ethnicity,
    COUNT(DISTINCT adm.admission_id) AS admission_count,
    COUNT(cus.care_unit_stay_id) AS care_unit_stay_count,
    MIN(adm.discharge_year) AS first_discharge_year,
    MAX(adm.discharge_year) AS last_discharge_year,
    SUM(CASE WHEN cus.unit_discharge_status = 'Expired' THEN 1 ELSE 0 END)
        AS expired_care_unit_stay_count
FROM patients AS pat
LEFT JOIN admissions AS adm
    ON adm.patient_id = pat.patient_id
LEFT JOIN care_unit_stays AS cus
    ON cus.admission_id = adm.admission_id
GROUP BY
    pat.patient_id,
    pat.gender,
    pat.ethnicity;

DROP VIEW IF EXISTS diagnosis_statistics;
CREATE VIEW diagnosis_statistics AS
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
GROUP BY COALESCE(diagnosis, '(missing)');

DROP VIEW IF EXISTS data_quality_report;
CREATE VIEW data_quality_report AS
SELECT 'import_rows' AS source_name, 'age' AS column_name, COUNT(*) AS missing_count
FROM import_rows WHERE age IS NULL
UNION ALL
SELECT 'import_rows', 'gender', COUNT(*)
FROM import_rows WHERE gender IS NULL
UNION ALL
SELECT 'import_rows', 'ethnicity', COUNT(*)
FROM import_rows WHERE ethnicity IS NULL
UNION ALL
SELECT 'import_rows', 'diagnosis', COUNT(*)
FROM import_rows WHERE diagnosis IS NULL
UNION ALL
SELECT 'import_rows', 'admission_height', COUNT(*)
FROM import_rows WHERE admission_height IS NULL
UNION ALL
SELECT 'import_rows', 'admission_weight', COUNT(*)
FROM import_rows WHERE admission_weight IS NULL
UNION ALL
SELECT 'import_rows', 'discharge_weight', COUNT(*)
FROM import_rows WHERE discharge_weight IS NULL
UNION ALL
SELECT 'import_rows', 'hospital_admit_source', COUNT(*)
FROM import_rows WHERE hospital_admit_source IS NULL;

DROP TRIGGER IF EXISTS trg_care_unit_stays_validate_visit_number;
CREATE TRIGGER trg_care_unit_stays_validate_visit_number
BEFORE INSERT ON care_unit_stays
FOR EACH ROW
WHEN NEW.unit_visit_number IS NOT NULL AND NEW.unit_visit_number < 1
BEGIN
    SELECT RAISE(ABORT, 'unit_visit_number must be >= 1');
END;
