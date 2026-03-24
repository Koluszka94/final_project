# Aplikacja analityczna danych pacjentów

Desktopowa aplikacja w Python + PyQt do importu danych medycznych z pliku CSV do bazy SQLite oraz wykonywania analiz na danych relacyjnych.

Projekt działa zgodnie z docelowym przepływem:

`CSV -> SQLite -> aplikacja analityczna w PyQt`

Plik CSV jest wyłącznie źródłem importu. Wszystkie filtry, analizy grup, widoki SQL i wykresy działają już na danych zapisanych w SQLite.

## Najważniejsze funkcje

- import `EHR.csv` do znormalizowanej bazy SQLite
- wyszukiwanie i filtrowanie danych pacjentów oraz pobytów
- porównywanie grup pacjentów z użyciem zapytań SQL
- korzystanie z widoków SQL i logiki bazodanowej
- osobna sekcja wykresów osadzona w GUI
- eksport wyników tabel do CSV
- eksport wykresów do PNG

## Technologie

- Python 3.10+
- PyQt6
- pandas
- matplotlib
- SQLite

## Struktura projektu

```text
app.py
analytics/
database/
gui/
queries/
services/
scripts/
tests/
EHR.csv
requirements.txt
README.md
```

## Warstwy aplikacji

- `app.py`
  Punkt startowy aplikacji.
- `gui/`
  Interfejs PyQt z sekcjami importu, przeglądania danych, analiz grup, wykresów i widoków SQL.
- `services/`
  Warstwa pośrednia między GUI a bazą danych.
- `queries/`
  Zapytania SQL do filtrowania, porównań grup i widoków.
- `analytics/`
  Zapytania agregujące i dane do wykresów.
- `database/`
  Połączenie SQLite, schemat relacyjny i import CSV.

## Przepływ danych

1. Użytkownik wybiera plik CSV w GUI.
2. Import uruchamia się w tle, aby nie blokować interfejsu.
3. Warstwa `database/importer.py` czyści i normalizuje dane wejściowe.
4. Schemat SQLite jest odtwarzany z `database/schema.sql`.
5. Dane trafiają do tabel relacyjnych i widoków SQL używanych przez aplikację.
6. GUI pobiera już tylko dane zapisane w SQLite.

## Reguły czyszczenia danych

- kolumna wieku:
  `>89` jest zamieniane na `89`, a końcowo pozostaje jedna kolumna `age`
- kolumny wag:
  wartości liczbowe są normalizowane do formatu numerycznego, także gdy wejście zawiera przecinek
- puste pola tekstowe:
  są uzupełniane wartością `brak danych`
- masa przy wypisie:
  jeśli brak wartości, przyjmowana jest masa przy przyjęciu
- zmiana masy:
  liczona automatycznie jako `masa przy wypisie - masa przy przyjęciu`
- typ oddziału:
  usuwane są dopiski w nawiasach
- rozpoznania:
  są mapowane do kontrolowanego, polskiego słownictwa medycznego

## Najważniejsze pliki do omówienia

- [app.py](./app.py)
  uruchomienie aplikacji PyQt
- [gui/main_window.py](./gui/main_window.py)
  główne okno, zakładki i obsługa importu w tle
- [services/ehr_service.py](./services/ehr_service.py)
  warstwa pośrednia między GUI a SQL
- [database/importer.py](./database/importer.py)
  logika importu CSV i czyszczenia danych
- [database/connection.py](./database/connection.py)
  połączenie z SQLite i odtwarzanie schematu
- [database/schema.sql](./database/schema.sql)
  definicja tabel, widoków i triggera
- [queries/patient_queries.py](./queries/patient_queries.py)
  wyszukiwanie i filtrowanie rekordów
- [analytics/group_comparison.py](./analytics/group_comparison.py)
  agregacje do analiz grupowych i wykresów

## Schemat bazy danych

Schemat SQLite jest zdefiniowany w [database/schema.sql](./database/schema.sql).
Wizualizacja ERD i zaleznosci widokow jest opisana w [database/schema_diagram.md](./database/schema_diagram.md).
Offline SVG bez zaleznosci od podgladu Mermaid jest dostepny w [database/schema_diagram.svg](./database/schema_diagram.svg).

### Tabele główne

- `import_rows`
- `patients`
- `admissions`
- `care_unit_stays`

### Widoki SQL

- `patient_overview`
- `patient_summary`
- `diagnosis_statistics`
- `data_quality_report`

### Logika bazy danych

- trigger `trg_care_unit_stays_validate_visit_number`

## Import danych

1. Użytkownik uruchamia aplikację.
2. W sekcji `Import danych` wybiera plik CSV.
3. Import uruchamia się automatycznie po wyborze pliku.
4. Dane są czyszczone, normalizowane i zapisywane do `ehr_app.db` w tle.
5. Dalsza praca odbywa się już wyłącznie na danych SQL.

## Sekcje GUI

- `Import danych`
  Wybór pliku CSV, automatyczny import w tle, raport importu i szybkie podsumowanie bazy.
- `Przegląd i filtrowanie`
  Wyszukiwanie i filtrowanie rekordów pacjentów oraz pobytów.
- `Analizy grup pacjentów`
  Porównania grup oraz statystyki rozpoznań.
- `Wykresy i wizualizacje`
  Osadzony panel wykresów oparty na wynikach SQL.
- `Statystyki i widoki SQL`
  Gotowe widoki bazy danych i raport jakości danych.

## Instalacja

```powershell
python -m pip install -r requirements.txt
```

## Uruchomienie na innym komputerze

Najprostsza kolejność kroków:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python app.py
```

Upewnij się, że plik `EHR.csv` znajduje się w katalogu projektu, bo aplikacja korzysta z niego jako źródła importu.

## Przygotowanie bazy testowej

Jeśli chcesz zbudować bazę przed uruchomieniem GUI:

```powershell
python scripts\prepare_demo_db.py --csv EHR.csv --db ehr_app.db
```

## Uruchomienie aplikacji

```powershell
python app.py
```

## Test automatyczny

Smoke test backendu:

```powershell
python -m unittest discover -s tests -v
```

Test sprawdza:

- import CSV do SQLite
- filtrowanie danych przez SQL
- analizy grupowe
- widoki SQL
- podstawowe metryki projektu

## Przykładowy scenariusz ręcznego testu

1. Uruchom `python app.py`.
2. W sekcji `Import danych` wskaż `EHR.csv`.
3. Poczekaj, aż automatyczny import w tle się zakończy.
4. Sprawdź, czy podsumowanie pokazuje:
   - `Pacjenci: 1091`
   - `Hospitalizacje: 1242`
   - `Pobyty oddziałowe: 1447`
5. W sekcji `Przegląd i filtrowanie` ustaw filtr `Płeć = Kobieta` i uruchom wyszukiwanie.
6. W sekcji `Analizy grup pacjentów` uruchom porównanie według `Status wypisu z oddziału`.
7. W sekcji `Statystyki i widoki SQL` wczytaj `Podsumowanie pacjentów`.
8. W sekcji `Wykresy i wizualizacje` wygeneruj:
   - histogram wieku,
   - wykres słupkowy liczby rekordów według statusu wypisu z oddziału.
9. Wyeksportuj jedną tabelę do CSV i jeden wykres do PNG.

## Uwagi

- interfejs użytkownika jest w całości po polsku
- kod, nazwy klas i plików pozostają techniczne i spójne po angielsku
- `ehr_app.db` jest plikiem roboczym i można go w każdej chwili odtworzyć z `EHR.csv`
