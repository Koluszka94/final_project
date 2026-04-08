# Aplikacja analityczna danych pacjentów

Desktopowa aplikacja w Pythonie służąca do importu danych medycznych z pliku CSV do bazy SQLite, a następnie do ich przeglądania, filtrowania, analizowania i wizualizacji w interfejsie PyQt.

Projekt został przygotowany tak, aby pokazać pełny przepływ pracy z danymi:

`CSV -> czyszczenie i normalizacja -> relacyjna baza SQLite -> zapytania SQL -> GUI`

Plik `EHR.csv` jest tylko źródłem wejściowym. Po imporcie cała dalsza praca odbywa się już na danych zapisanych w bazie relacyjnej.

## Cel projektu

Projekt prezentuje:

- import i czyszczenie danych medycznych z pliku CSV,
- projekt relacyjnej bazy danych w SQLite,
- wykorzystanie widoków SQL i prostych mechanizmów walidacji w bazie,
- warstwową architekturę aplikacji desktopowej,
- analizy grup pacjentów oraz wykresy generowane na podstawie danych SQL.

## Najważniejsze funkcje

- import `EHR.csv` do znormalizowanej bazy `ehr_app.db`,
- automatyczne czyszczenie i ujednolicanie danych podczas importu,
- wyszukiwanie i filtrowanie rekordów pacjentów oraz pobytów oddziałowych,
- porównywanie grup pacjentów według wybranych cech,
- korzystanie z gotowych widoków SQL,
- generowanie wykresów w aplikacji,
- eksport tabel do CSV i wykresów do PNG.

## Technologie

- Python 3.10+
- PyQt6
- pandas
- matplotlib
- SQLite

## Architektura projektu

```text
app.py                 # punkt startowy aplikacji
analytics/             # agregacje do analiz grup i wykresów
database/              # schemat SQLite, połączenie i import CSV
gui/                   # interfejs PyQt
queries/               # zapytania SQL do przeglądania danych i widoków
services/              # warstwa pośrednia między GUI a bazą
scripts/               # pomocnicze skrypty CLI
tests/                 # testy smoke backendu
EHR.csv                # przykładowy zbiór wejściowy
```

### Warstwy aplikacji

- `app.py`
  Uruchamia aplikację PyQt i tworzy główne okno.
- `gui/`
  Zawiera interfejs użytkownika z zakładkami importu, przeglądu danych, analiz, wykresów i widoków SQL.
- `services/`
  Udostępnia jedno API dla GUI i spina import, zapytania oraz analizy.
- `queries/`
  Zawiera zapytania SQL do filtrowania rekordów i odczytu gotowych widoków.
- `analytics/`
  Zawiera agregacje do porównań grup oraz źródła danych do wykresów.
- `database/`
  Odpowiada za połączenie z SQLite, definicję schematu i proces importu CSV.

## Przepływ danych

1. Użytkownik wybiera plik CSV w aplikacji.
2. Import uruchamia się w tle, aby nie blokować interfejsu.
3. Dane są czyszczone i normalizowane w `database/importer.py`.
4. Schemat bazy jest odtwarzany na podstawie `database/schema.sql`.
5. Dane trafiają do tabel relacyjnych i widoków SQL.
6. GUI korzysta już wyłącznie z danych zapisanych w SQLite.

## Model danych

Schemat bazy danych znajduje się w [database/schema.sql](./database/schema.sql), a opis relacji i widoków w [database/schema_diagram.md](./database/schema_diagram.md).

### Tabele główne

- `import_rows` - tabela stagingowa przechowująca oczyszczone dane po imporcie,
- `patients` - dane pacjentów,
- `admissions` - hospitalizacje,
- `care_unit_stays` - pobyty oddziałowe powiązane z hospitalizacją.

### Widoki SQL

- `patient_overview` - główny widok analityczny do filtrowania i wyszukiwania,
- `patient_summary` - agregacja na poziomie pacjenta,
- `diagnosis_statistics` - statystyki rozpoznań,
- `data_quality_report` - raport braków danych po imporcie.

### Logika bazy

- trigger `trg_care_unit_stays_validate_visit_number`, który blokuje zapis niepoprawnego `unit_visit_number`.

## Reguły czyszczenia danych

Podczas importu aplikacja stosuje zestaw prostych, jawnych reguł:

- wartość wieku `>89` jest zamieniana na `89`,
- wartości liczbowe z przecinkiem są zamieniane na poprawny format numeryczny,
- puste pola tekstowe są uzupełniane wartością `brak danych`,
- brakująca masa przy wypisie dziedziczy wartość masy przy przyjęciu,
- `weight_change_kg` jest wyliczane automatycznie,
- z nazw typów oddziałów usuwane są dopiski w nawiasach,
- rozpoznania są tłumaczone i mapowane do spójnego, polskiego nazewnictwa.

## Interfejs użytkownika

Aplikacja składa się z pięciu głównych sekcji:

- `Import danych`
  Wybór pliku CSV, automatyczny import w tle oraz podsumowanie wyniku importu.
- `Przegląd i filtrowanie`
  Wyszukiwanie rekordów pacjentów i pobytów według zestawu filtrów.
- `Analizy grup pacjentów`
  Porównania grup i statystyki rozpoznań.
- `Wykresy i wizualizacje`
  Histogramy i wykresy słupkowe tworzone na podstawie danych SQL.
- `Statystyki i widoki SQL`
  Odczyt gotowych widoków analitycznych i raportu jakości danych.

## Uruchomienie projektu

### Instalacja zależności

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Uruchomienie aplikacji

```powershell
python app.py
```

Warunek: plik `EHR.csv` powinien znajdować się w katalogu projektu.

### Przygotowanie bazy bez uruchamiania GUI

```powershell
python scripts\prepare_demo_db.py --csv EHR.csv --db ehr_app.db
```

## Testy

Smoke test backendu można uruchomić poleceniem:

```powershell
python -m unittest discover -s tests -v
```

Zakres testów obejmuje:

- import CSV do SQLite,
- gotowość i zgodność schematu bazy,
- filtrowanie danych przez SQL,
- analizy grupowe,
- odczyt widoków SQL,
- podstawowe metryki po imporcie.

## Przykładowy scenariusz prezentacji

1. Uruchom `python app.py`.
2. W sekcji `Import danych` wybierz plik `EHR.csv`.
3. Po zakończeniu importu sprawdź, czy podsumowanie pokazuje:
   - `Pacjenci: 1091`
   - `Hospitalizacje: 1242`
   - `Pobyty oddziałowe: 1447`
4. W zakładce `Przegląd i filtrowanie` ustaw filtr `Płeć = Kobieta` i uruchom wyszukiwanie.
5. W zakładce `Analizy grup pacjentów` wykonaj porównanie według `Status wypisu z oddziału`.
6. W zakładce `Statystyki i widoki SQL` wczytaj `Podsumowanie pacjentów`.
7. W zakładce `Wykresy i wizualizacje` wygeneruj histogram wieku lub wykres słupkowy dla wybranej grupy.
8. Wyeksportuj jedną tabelę do CSV oraz jeden wykres do PNG.

## Najważniejsze pliki

- [app.py](./app.py) - punkt startowy aplikacji,
- [gui/main_window.py](./gui/main_window.py) - główne okno i obsługa zakładek,
- [gui/filter_panel.py](./gui/filter_panel.py) - współdzielony panel filtrów,
- [services/ehr_service.py](./services/ehr_service.py) - fasada łącząca GUI z bazą danych,
- [database/importer.py](./database/importer.py) - logika importu i czyszczenia danych,
- [database/connection.py](./database/connection.py) - połączenie z SQLite i odbudowa schematu,
- [database/schema.sql](./database/schema.sql) - definicja tabel, widoków i triggera,
- [queries/patient_queries.py](./queries/patient_queries.py) - filtrowanie i metryki przeglądu,
- [analytics/group_comparison.py](./analytics/group_comparison.py) - agregacje do analiz i wykresów,
- [tests/test_backend_smoke.py](./tests/test_backend_smoke.py) - testy integracyjne backendu.

## Uwagi

- interfejs użytkownika jest w całości po polsku,
- kod źródłowy, nazwy modułów i klas pozostają techniczne i spójne po angielsku,
- `ehr_app.db` jest plikiem roboczym i może zostać odtworzony z `EHR.csv` w dowolnym momencie.
