from __future__ import annotations

"""Pomocniczy skrypt CLI do odbudowy bazy SQLite z pliku CSV."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.importer import import_csv_to_database


def build_parser() -> argparse.ArgumentParser:
    """Tworzy parser argumentów dla lokalnego przygotowania bazy demonstracyjnej."""
    parser = argparse.ArgumentParser(
        description="Import danych EHR z CSV do bazy SQLite na potrzeby lokalnych testów."
    )
    parser.add_argument(
        "--csv",
        default="EHR.csv",
        help="Ścieżka do pliku CSV.",
    )
    parser.add_argument(
        "--db",
        default="ehr_app.db",
        help="Ścieżka do docelowej bazy SQLite.",
    )
    return parser


def main() -> None:
    """Uruchamia samodzielny import CSV -> SQLite z poziomu terminala."""
    parser = build_parser()
    args = parser.parse_args()

    csv_path = Path(args.csv)
    db_path = args.db

    stats = import_csv_to_database(csv_path, db_path)

    if db_path == ":memory:" or str(db_path).startswith("file:"):
        db_display = str(db_path)
    else:
        db_display = str(Path(db_path).resolve())

    print("Baza została przygotowana poprawnie.")
    print(f"Plik CSV: {csv_path.resolve()}")
    print(f"Baza SQLite: {db_display}")
    print(f"Liczba wierszy wejściowych: {stats.raw_rows}")
    print(f"Liczba pacjentów: {stats.patients}")
    print(f"Liczba hospitalizacji: {stats.hospital_stays}")
    print(f"Liczba pobytów oddziałowych: {stats.unit_stays}")
    print(f"Konflikty etniczności pacjentów: {stats.patient_ethnicity_conflicts}")
    print(f"Konflikty wieku pacjentów: {stats.patient_age_conflicts}")
    print(f"Konflikty rozpoznań hospitalizacji: {stats.hospital_diagnosis_conflicts}")


if __name__ == "__main__":
    main()
