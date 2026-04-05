from __future__ import annotations

"""Testy smoke backendu obejmujące import, schemat i zapytania SQL."""

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from database.connection import get_connection
from database.importer import CSV_COLUMNS
from services.ehr_service import EHRService
from queries.patient_queries import SearchFilters


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "EHR.csv"


class BackendSmokeTest(unittest.TestCase):
    """Testy integracyjne dla importu CSV i warstwy analitycznej backendu."""

    def setUp(self) -> None:
        self.db_uri = "file:test_backend_smoke?mode=memory&cache=shared"
        self.anchor_connection = sqlite3.connect(self.db_uri, uri=True)
        self.service = EHRService(self.db_uri)

    def tearDown(self) -> None:
        self.anchor_connection.close()

    def test_database_ready_is_false_without_current_schema(self) -> None:
        """Zwykły plik SQLite nie powinien być uznany za gotową bazę aplikacji."""
        empty_db_uri = "file:test_backend_empty?mode=memory&cache=shared"
        empty_anchor = sqlite3.connect(empty_db_uri, uri=True)
        try:
            service = EHRService(empty_db_uri)
            self.assertTrue(service.database_exists())
            self.assertFalse(service.database_ready())
        finally:
            empty_anchor.close()

    def test_database_ready_is_false_for_stale_schema(self) -> None:
        """Starsze lub niepełne schematy muszą być odrzucane przez warstwę serwisową."""
        stale_db_uri = "file:test_backend_stale?mode=memory&cache=shared"
        stale_anchor = sqlite3.connect(stale_db_uri, uri=True)
        try:
            stale_anchor.executescript(
                """
                CREATE TABLE patients (
                    uniquepid TEXT PRIMARY KEY
                );
                CREATE TABLE admissions (
                    admission_id INTEGER PRIMARY KEY
                );
                CREATE TABLE care_unit_stays (
                    care_unit_stay_id INTEGER PRIMARY KEY
                );
                CREATE VIEW patient_overview AS
                SELECT 1 AS placeholder;
                """
            )
            service = EHRService(stale_db_uri)
            self.assertTrue(service.database_exists())
            self.assertFalse(service.database_ready())
        finally:
            stale_anchor.close()

    def test_import_search_group_and_views(self) -> None:
        """Pełny przepływ backendu powinien działać na dołączonym zbiorze EHR."""
        stats = self.service.import_csv(CSV_PATH)

        self.assertEqual(stats.raw_rows, 1447)
        self.assertEqual(stats.patients, 1091)
        self.assertEqual(stats.hospital_stays, 1242)
        self.assertEqual(stats.unit_stays, 1447)

        search_rows = self.service.search_records(SearchFilters(gender="Female", limit=10))
        self.assertFalse(search_rows.empty)
        self.assertTrue((search_rows["gender"] == "Female").all())

        overview = self.service.load_overview_metrics()
        self.assertEqual(overview["patient_count"], 1091)
        self.assertEqual(overview["admission_count"], 1242)
        self.assertEqual(overview["care_unit_stay_count"], 1447)

        grouped = self.service.load_group_summary(
            "unit_discharge_status",
            SearchFilters(limit=25),
        )
        self.assertFalse(grouped.empty)
        self.assertIn("group_value", grouped.columns)
        self.assertIn("record_count", grouped.columns)

        patient_view = self.service.load_view_data("patient_summary", limit=20)
        self.assertFalse(patient_view.empty)
        self.assertIn("patient_id", patient_view.columns)
        self.assertIn("care_unit_stay_count", patient_view.columns)

        diagnosis_view = self.service.load_view_data("diagnosis_summary", limit=20)
        self.assertFalse(diagnosis_view.empty)
        self.assertIn("diagnosis", diagnosis_view.columns)

        quality_view = self.service.load_view_data("data_quality_summary")
        self.assertFalse(quality_view.empty)
        self.assertIn("column_name", quality_view.columns)
        self.assertIn("missing_count", quality_view.columns)

    def test_import_cleans_and_normalizes_fields(self) -> None:
        """Reguły czyszczenia projektu powinny być stosowane podczas importu."""
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sample.csv"
            row = {column: None for column in CSV_COLUMNS}
            row.update(
                {
                    "patientunitstayid": 1,
                    "patienthealthsystemstayid": 10,
                    "gender": "",
                    "age": ">89",
                    "ethnicity": "",
                    "hospitalid": 3,
                    "wardid": 12,
                    "apacheadmissiondx": "Sepsis, pulmonary",
                    "admissionheight": "170,5",
                    "hospitaladmittime24": "",
                    "hospitaladmitoffset": 0,
                    "hospitaladmitsource": "",
                    "hospitaldischargeyear": 2024,
                    "hospitaldischargetime24": "",
                    "hospitaldischargeoffset": 120,
                    "hospitaldischargelocation": "",
                    "hospitaldischargestatus": "Alive",
                    "unittype": "Cardiac Surgery (CSICU)",
                    "unitadmittime24": "",
                    "unitadmitsource": "",
                    "unitvisitnumber": 1,
                    "unitstaytype": "",
                    "admissionweight": "70,5",
                    "dischargeweight": "",
                    "unitdischargetime24": "",
                    "unitdischargeoffset": 12,
                    "unitdischargelocation": "",
                    "unitdischargestatus": "Alive",
                    "uniquepid": "PAC-1",
                }
            )
            pd.DataFrame([row]).to_csv(csv_path, index=False)

            stats = self.service.import_csv(csv_path)
            self.assertEqual(stats.raw_rows, 1)

            search_rows = self.service.search_records(SearchFilters(limit=10))
            self.assertEqual(int(search_rows.loc[0, "age"]), 89)
            self.assertEqual(search_rows.loc[0, "diagnosis"], "Sepsa pochodzenia płucnego")
            self.assertEqual(search_rows.loc[0, "unit_type"], "Cardiac Surgery")
            self.assertAlmostEqual(float(search_rows.loc[0, "admission_weight"]), 70.5)
            self.assertAlmostEqual(float(search_rows.loc[0, "discharge_weight"]), 70.5)
            self.assertAlmostEqual(float(search_rows.loc[0, "weight_change_kg"]), 0.0)

            with get_connection(self.db_uri) as connection:
                admission_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(admissions)").fetchall()
                }
                self.assertIn("age", admission_columns)
                self.assertNotIn("age_raw", admission_columns)
                self.assertNotIn("age_value", admission_columns)

                imported_row = connection.execute(
                    """
                    SELECT
                        gender,
                        ethnicity,
                        hospital_admit_source,
                        unit_admit_source,
                        unit_stay_type
                    FROM import_rows
                    LIMIT 1
                    """
                ).fetchone()

                self.assertEqual(imported_row["gender"], "brak danych")
                self.assertEqual(imported_row["ethnicity"], "brak danych")
                self.assertEqual(imported_row["hospital_admit_source"], "brak danych")
                self.assertEqual(imported_row["unit_admit_source"], "brak danych")
                self.assertEqual(imported_row["unit_stay_type"], "brak danych")

    def test_import_overwrites_file_database_without_renaming_locked_file(self) -> None:
        """Powtórny import powinien działać dla plikowej bazy danych na Windows."""
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ehr_app.db"
            service = EHRService(db_path)
            external_connection = sqlite3.connect(db_path)
            try:
                external_connection.execute("PRAGMA foreign_keys = ON;")

                first_stats = service.import_csv(CSV_PATH)
                self.assertEqual(first_stats.raw_rows, 1447)

                second_stats = service.import_csv(CSV_PATH)
                self.assertEqual(second_stats.raw_rows, 1447)

                imported_count = external_connection.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name = 'patients'"
                ).fetchone()[0]
                self.assertEqual(imported_count, 1)
            finally:
                external_connection.close()

    def test_import_rebuilds_stale_file_schema(self) -> None:
        """Import powinien odbudować stary schemat plikowej bazy przed zapisem danych."""
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ehr_app.db"
            stale_connection = sqlite3.connect(db_path)
            try:
                stale_connection.executescript(
                    """
                    CREATE TABLE import_rows (
                        import_row_id INTEGER PRIMARY KEY,
                        age_raw TEXT
                    );
                    CREATE TABLE patients (
                        patient_id TEXT PRIMARY KEY
                    );
                    CREATE TABLE admissions (
                        admission_id INTEGER PRIMARY KEY,
                        patient_id TEXT,
                        age_raw TEXT,
                        age_value INTEGER
                    );
                    CREATE TABLE care_unit_stays (
                        care_unit_stay_id INTEGER PRIMARY KEY,
                        admission_id INTEGER,
                        diagnosis TEXT
                    );
                    """
                )
                stale_connection.commit()
            finally:
                stale_connection.close()

            service = EHRService(db_path)
            stats = service.import_csv(CSV_PATH)
            self.assertEqual(stats.raw_rows, 1447)

            with get_connection(db_path) as connection:
                import_rows_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(import_rows)").fetchall()
                }
                self.assertIn("age", import_rows_columns)
                self.assertNotIn("age_raw", import_rows_columns)


if __name__ == "__main__":
    unittest.main()
