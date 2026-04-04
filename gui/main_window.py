from __future__ import annotations

"""Główne okno PyQt do importu CSV, przeglądania danych i analiz SQL."""

from pathlib import Path

import pandas as pd
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.figure import Figure
from PyQt6.QtCore import QObject, QThread, Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFileDialog,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QStatusBar,
    QTabWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
    QSplitter,
)

from analytics.group_comparison import BAR_METRICS, HISTOGRAM_METRICS
from gui.filter_panel import FilterPanel
from gui.labels import get_value_label
from gui.table_model import PandasTableModel
from services.ehr_service import (
    DEFAULT_DB_PATH,
    EHRService,
    GROUP_BY_OPTIONS,
    GROUP_METRIC_OPTIONS,
    HISTOGRAM_OPTIONS,
    VIEW_OPTIONS,
)


VIEW_DESCRIPTIONS = {
    "patient_summary": (
        "Widok SQL prezentujący jedną linię na pacjenta oraz liczbę jego hospitalizacji "
        "i pobytów oddziałowych."
    ),
    "diagnosis_summary": (
        "Widok SQL agregujący najczęstsze rozpoznania oraz podstawowe miary "
        "czasów pobytu i śmiertelności."
    ),
    "data_quality_summary": (
        "Widok SQL pokazujący braki danych w zaimportowanym zbiorze wejściowym."
    ),
}


class ImportWorker(QObject):
    """Uruchamia import CSV poza głównym wątkiem GUI, aby nie blokować interfejsu."""

    finished = pyqtSignal(object, str)
    failed = pyqtSignal(str)

    def __init__(self, service: EHRService, csv_path: str):
        super().__init__()
        self._service = service
        self._csv_path = csv_path

    def run(self) -> None:
        try:
            stats = self._service.import_csv(self._csv_path)
        except Exception as error:  # pragma: no cover - GUI thread handoff
            self.failed.emit(str(error))
            return

        self.finished.emit(stats, self._csv_path)


class ChartCanvas(FigureCanvasQTAgg):
    """Pomocnicza otoczka dla wykresu Matplotlib osadzonego w PyQt."""

    def __init__(self, parent=None):
        self.figure = Figure(figsize=(9, 5))
        self.axes = self.figure.add_subplot(111)
        super().__init__(self.figure)
        self.setParent(parent)

    def draw_placeholder(self, message: str) -> None:
        self.axes.clear()
        self.axes.text(
            0.5,
            0.5,
            message,
            ha="center",
            va="center",
            fontsize=12,
            transform=self.axes.transAxes,
        )
        self.axes.set_axis_off()
        self.figure.tight_layout()
        self.draw()

    def draw_histogram(self, frame: pd.DataFrame, metric: str, bins: int) -> None:
        self.axes.clear()
        self.axes.hist(
            frame["metric_value"],
            bins=max(5, bins),
            color="#2a9d8f",
            edgecolor="white",
        )
        self.axes.set_title(f"Histogram: {HISTOGRAM_METRICS[metric]}")
        self.axes.set_xlabel(HISTOGRAM_METRICS[metric])
        self.axes.set_ylabel("Liczba rekordów")
        self.axes.grid(axis="y", alpha=0.25)
        self.figure.tight_layout()
        self.draw()

    def draw_group_bar(self, frame: pd.DataFrame, metric: str, group_label: str) -> None:
        self.axes.clear()
        ordered = frame.iloc[::-1]
        self.axes.barh(ordered["group_value"], ordered["metric_value"], color="#264653")
        self.axes.set_title(f"{BAR_METRICS[metric]} według: {group_label}")
        self.axes.set_xlabel(BAR_METRICS[metric])
        self.axes.set_ylabel(group_label)
        self.axes.grid(axis="x", alpha=0.25)
        self.figure.tight_layout()
        self.draw()

    def has_chart_data(self) -> bool:
        return self.axes.has_data()


class MainWindow(QMainWindow):
    """Główne okno aplikacji z zakładkami importu, przeglądu i analiz."""

    def __init__(self, service: EHRService | None = None):
        super().__init__()
        self.service = service or EHRService(DEFAULT_DB_PATH)
        self.import_thread: QThread | None = None
        self.import_worker: ImportWorker | None = None
        self.import_in_progress = False

        self.setWindowTitle("Analityka danych pacjentów")
        self.resize(1520, 940)
        self.setMinimumSize(1280, 800)
        self.setStatusBar(QStatusBar())
        self._apply_styles()

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.import_tab = self._build_import_tab()
        self.search_tab = self._build_search_tab()
        self.group_tab = self._build_group_tab()
        self.charts_tab = self._build_charts_tab()
        self.views_tab = self._build_views_tab()

        self.tabs.addTab(self.import_tab, "Import danych")
        self.tabs.addTab(self.search_tab, "Przegląd i filtrowanie")
        self.tabs.addTab(self.group_tab, "Analizy grup pacjentów")
        self.tabs.addTab(self.charts_tab, "Wykresy i wizualizacje")
        self.tabs.addTab(self.views_tab, "Statystyki i widoki SQL")

        self.refresh_application_state(run_default_search=self.service.database_ready())

    def _build_import_tab(self) -> QWidget:
        """Buduje uproszczoną zakładkę importu używaną w finalnym przepływie."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        intro_box = QGroupBox("Import danych z pliku CSV")
        intro_layout = QVBoxLayout(intro_box)
        intro_label = QLabel(
            "Wybierz plik CSV z danymi medycznymi. Aplikacja automatycznie "
            "oczyści dane, zapisze je w tle i odświeży widoki analityczne."
        )
        intro_label.setWordWrap(True)
        intro_layout.addWidget(intro_label)

        file_box = QGroupBox("Import z pliku CSV")
        file_layout = QVBoxLayout(file_box)
        file_row = QHBoxLayout()

        self.le_csv_path = QLineEdit()
        self.le_csv_path.setReadOnly(True)
        self.le_csv_path.setPlaceholderText("Nie wybrano pliku CSV")

        self.btn_csv = QPushButton("Wybierz plik CSV")
        self.btn_csv.clicked.connect(self.on_browse_csv)

        file_row.addWidget(QLabel("Plik CSV"))
        file_row.addWidget(self.le_csv_path, 1)
        file_row.addWidget(self.btn_csv)
        file_layout.addLayout(file_row)

        auto_import_note = QLabel(
            "Po wyborze pliku import rozpocznie się automatycznie. Nie są wymagane "
            "żadne dodatkowe kroki techniczne."
        )
        auto_import_note.setWordWrap(True)
        file_layout.addWidget(auto_import_note)

        status_box = QGroupBox("Status importu")
        status_layout = QVBoxLayout(status_box)
        self.lbl_import_status = QLabel("Wybierz plik CSV, aby rozpocząć import danych.")
        self.lbl_import_status.setWordWrap(True)
        self.import_progress = QProgressBar()
        self.import_progress.setTextVisible(False)
        self.import_progress.hide()
        status_layout.addWidget(self.lbl_import_status)
        status_layout.addWidget(self.import_progress)

        overview_box = QGroupBox("Szybkie podsumowanie bazy")
        overview_layout = QHBoxLayout(overview_box)
        patient_card, self.lbl_patients = self._create_metric_card("Pacjenci")
        admission_card, self.lbl_admissions = self._create_metric_card("Hospitalizacje")
        stay_card, self.lbl_unit_stays = self._create_metric_card("Pobyty oddziałowe")
        unit_mortality_card, self.lbl_unit_mortality = self._create_metric_card(
            "Śmiertelność oddziałowa"
        )
        hospital_mortality_card, self.lbl_hospital_mortality = self._create_metric_card(
            "Śmiertelność szpitalna"
        )
        for widget in [
            patient_card,
            admission_card,
            stay_card,
            unit_mortality_card,
            hospital_mortality_card,
        ]:
            overview_layout.addWidget(widget)

        log_box = QGroupBox("Podsumowanie ostatniego importu")
        log_layout = QVBoxLayout(log_box)
        self.import_log = QPlainTextEdit()
        self.import_log.setReadOnly(True)
        self.import_log.setPlaceholderText(
            "Po zakończeniu importu pojawi się tutaj krótkie podsumowanie danych."
        )
        log_layout.addWidget(self.import_log)

        layout.addWidget(intro_box)
        layout.addWidget(file_box)
        layout.addWidget(status_box)
        layout.addWidget(overview_box)
        layout.addWidget(log_box, 1)
        return tab

    def _build_search_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.search_filters = FilterPanel()
        splitter.addWidget(self._build_filter_container(self.search_filters, "Filtry wyszukiwania"))

        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        header = QLabel(
            "Przeglądaj pacjentów i pobyty oddziałowe przy użyciu filtrów opartych o dane SQL."
        )
        header.setWordWrap(True)
        results_layout.addWidget(header)

        search_buttons = QHBoxLayout()
        btn_search = QPushButton("Wyszukaj dane")
        btn_search.clicked.connect(self.on_search_records)
        btn_export = QPushButton("Eksportuj wynik")
        btn_export.clicked.connect(self.on_export_search_results)
        self._mark_secondary(btn_export)
        btn_reset = QPushButton("Wyczyść filtry")
        btn_reset.clicked.connect(self.on_reset_search_filters)
        self._mark_secondary(btn_reset)
        search_buttons.addWidget(btn_search)
        search_buttons.addWidget(btn_export)
        search_buttons.addWidget(btn_reset)
        search_buttons.addStretch()
        results_layout.addLayout(search_buttons)

        self.search_table = QTableView()
        self.search_model = PandasTableModel()
        self.search_table.setModel(self.search_model)
        self._configure_table(self.search_table)

        results_box = QGroupBox("Wyniki wyszukiwania")
        results_box_layout = QVBoxLayout(results_box)
        results_box_layout.addWidget(self.search_table)
        results_layout.addWidget(results_box, 1)

        splitter.addWidget(results_widget)
        splitter.setSizes([340, 1100])

        layout.addWidget(splitter)
        return tab
    def _build_group_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.group_filters = FilterPanel()
        splitter.addWidget(self._build_filter_container(self.group_filters, "Filtry analizy"))

        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)

        info_label = QLabel(
            "Wybierz rodzaj analizy i uruchom zapytanie SQL dla wybranej grupy pacjentów."
        )
        info_label.setWordWrap(True)
        results_layout.addWidget(info_label)

        controls_box = QGroupBox("Ustawienia analizy")
        controls = QHBoxLayout(controls_box)
        self.cb_analysis_type = QComboBox()
        self.cb_analysis_type.addItem("Porównanie grup pacjentów", "group_comparison")
        self.cb_analysis_type.addItem("Najczęstsze rozpoznania", "diagnosis_summary")
        self.cb_analysis_type.currentIndexChanged.connect(self._update_group_controls_state)

        self.cb_group_by = QComboBox()
        for key, label in GROUP_BY_OPTIONS:
            self.cb_group_by.addItem(label, key)

        self.spin_diagnosis_limit = QSpinBox()
        self.spin_diagnosis_limit.setRange(5, 100)
        self.spin_diagnosis_limit.setValue(20)

        btn_run = QPushButton("Uruchom analizę")
        btn_run.clicked.connect(self.on_run_group_analysis)
        btn_export = QPushButton("Eksportuj wynik")
        btn_export.clicked.connect(self.on_export_group_results)
        self._mark_secondary(btn_export)
        btn_reset = QPushButton("Wyczyść filtry")
        btn_reset.clicked.connect(self.on_reset_group_filters)
        self._mark_secondary(btn_reset)

        controls.addWidget(QLabel("Rodzaj analizy"))
        controls.addWidget(self.cb_analysis_type)
        controls.addWidget(QLabel("Grupuj według"))
        controls.addWidget(self.cb_group_by)
        controls.addWidget(QLabel("Limit rozpoznań"))
        controls.addWidget(self.spin_diagnosis_limit)
        controls.addWidget(btn_run)
        controls.addWidget(btn_export)
        controls.addWidget(btn_reset)
        controls.addStretch()
        results_layout.addWidget(controls_box)

        self.lbl_analysis_hint = QLabel()
        self.lbl_analysis_hint.setWordWrap(True)
        results_layout.addWidget(self.lbl_analysis_hint)

        self.group_table = QTableView()
        self.group_model = PandasTableModel()
        self.group_table.setModel(self.group_model)
        self._configure_table(self.group_table)

        results_box = QGroupBox("Wyniki analiz")
        results_box_layout = QVBoxLayout(results_box)
        results_box_layout.addWidget(self.group_table)
        results_layout.addWidget(results_box, 1)

        splitter.addWidget(results_widget)
        splitter.setSizes([340, 1100])
        layout.addWidget(splitter)

        self._update_group_controls_state()
        return tab

    def _build_charts_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.chart_filters = FilterPanel()

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.addWidget(self._build_filter_container(self.chart_filters, "Filtry wykresu"))

        controls_box = QGroupBox("Ustawienia wykresu")
        controls_layout = QVBoxLayout(controls_box)

        row_one = QHBoxLayout()
        self.cb_chart_type = QComboBox()
        self.cb_chart_type.addItem("Histogram", "histogram")
        self.cb_chart_type.addItem("Wykres słupkowy grup", "group_bar")
        self.cb_chart_type.currentIndexChanged.connect(self._update_chart_controls_state)
        row_one.addWidget(QLabel("Typ wykresu"))
        row_one.addWidget(self.cb_chart_type)
        controls_layout.addLayout(row_one)

        row_two = QHBoxLayout()
        self.cb_hist_metric = QComboBox()
        for key, label in HISTOGRAM_OPTIONS:
            self.cb_hist_metric.addItem(label, key)
        self.cb_chart_group_by = QComboBox()
        for key, label in GROUP_BY_OPTIONS:
            self.cb_chart_group_by.addItem(label, key)
        row_two.addWidget(QLabel("Parametr histogramu"))
        row_two.addWidget(self.cb_hist_metric)
        row_two.addWidget(QLabel("Grupowanie"))
        row_two.addWidget(self.cb_chart_group_by)
        controls_layout.addLayout(row_two)

        row_three = QHBoxLayout()
        self.cb_group_metric = QComboBox()
        for key, label in GROUP_METRIC_OPTIONS:
            self.cb_group_metric.addItem(label, key)
        self.spin_bins = QSpinBox()
        self.spin_bins.setRange(5, 100)
        self.spin_bins.setValue(20)
        row_three.addWidget(QLabel("Miara dla grup"))
        row_three.addWidget(self.cb_group_metric)
        row_three.addWidget(QLabel("Liczba przedziałów"))
        row_three.addWidget(self.spin_bins)
        controls_layout.addLayout(row_three)

        row_four = QHBoxLayout()
        btn_draw = QPushButton("Generuj wykres")
        btn_draw.clicked.connect(self.on_draw_chart)
        btn_export_chart = QPushButton("Eksportuj wykres")
        btn_export_chart.clicked.connect(self.on_export_chart)
        self._mark_secondary(btn_export_chart)
        btn_reset = QPushButton("Wyczyść filtry")
        btn_reset.clicked.connect(self.on_reset_chart_filters)
        self._mark_secondary(btn_reset)
        row_four.addWidget(btn_draw)
        row_four.addWidget(btn_export_chart)
        row_four.addWidget(btn_reset)
        row_four.addStretch()
        controls_layout.addLayout(row_four)

        left_layout.addWidget(controls_box)
        left_layout.addStretch()
        splitter.addWidget(left_panel)

        chart_box = QGroupBox("Obszar wykresu")
        chart_layout = QVBoxLayout(chart_box)
        chart_info = QLabel(
            "Wykres jest generowany na podstawie wyników zapytań SQL i osadzony bezpośrednio w aplikacji."
        )
        chart_info.setWordWrap(True)
        chart_layout.addWidget(chart_info)

        self.chart_canvas = ChartCanvas()
        self.chart_canvas.draw_placeholder("Zaimportuj dane i wygeneruj wykres.")
        chart_layout.addWidget(self.chart_canvas, 1)
        splitter.addWidget(chart_box)
        splitter.setSizes([420, 1020])

        layout.addWidget(splitter)
        self._update_chart_controls_state()
        return tab

    def _build_views_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        info_box = QGroupBox("Statystyki i widoki SQL")
        info_layout = QVBoxLayout(info_box)
        info_layout.addWidget(
            QLabel(
                "Ta sekcja pokazuje gotowe widoki bazy danych oraz raport jakości danych "
                "przygotowane do celów analitycznych i projektowych."
            )
        )

        controls_box = QGroupBox("Przegląd widoków")
        controls = QHBoxLayout(controls_box)
        self.cb_view_name = QComboBox()
        for key, label in VIEW_OPTIONS:
            self.cb_view_name.addItem(label, key)
        self.cb_view_name.currentIndexChanged.connect(self._update_view_controls_state)

        self.spin_view_limit = QSpinBox()
        self.spin_view_limit.setRange(10, 5000)
        self.spin_view_limit.setValue(250)

        btn_load_view = QPushButton("Wczytaj widok")
        btn_load_view.clicked.connect(self.on_load_view)
        btn_export_view = QPushButton("Eksportuj wynik")
        btn_export_view.clicked.connect(self.on_export_view_results)
        self._mark_secondary(btn_export_view)
        btn_reset_view = QPushButton("Wyczyść wybór")
        btn_reset_view.clicked.connect(self.on_reset_view_selection)
        self._mark_secondary(btn_reset_view)

        controls.addWidget(QLabel("Widok"))
        controls.addWidget(self.cb_view_name)
        controls.addWidget(QLabel("Limit"))
        controls.addWidget(self.spin_view_limit)
        controls.addWidget(btn_load_view)
        controls.addWidget(btn_export_view)
        controls.addWidget(btn_reset_view)
        controls.addStretch()

        description_box = QGroupBox("Opis widoku")
        description_layout = QVBoxLayout(description_box)
        self.lbl_view_description = QLabel()
        self.lbl_view_description.setWordWrap(True)
        description_layout.addWidget(self.lbl_view_description)

        self.views_table = QTableView()
        self.views_model = PandasTableModel()
        self.views_table.setModel(self.views_model)
        self._configure_table(self.views_table)

        views_box = QGroupBox("Wyniki widoku SQL")
        views_layout = QVBoxLayout(views_box)
        views_layout.addWidget(self.views_table)

        layout.addWidget(info_box)
        layout.addWidget(controls_box)
        layout.addWidget(description_box)
        layout.addWidget(views_box, 1)

        self._update_view_controls_state()
        return tab

    def refresh_application_state(self, run_default_search: bool = False) -> None:
        """Synchronizuje wszystkie zakładki z aktualnym stanem danych w SQLite."""
        if not self.service.database_ready():
            self.tabs.setCurrentIndex(0)
            self._set_data_tabs_enabled(False)
            self._set_overview_labels({})
            self.search_model.set_dataframe(pd.DataFrame())
            self.group_model.set_dataframe(pd.DataFrame())
            self.views_model.set_dataframe(pd.DataFrame())
            self.chart_canvas.draw_placeholder("Zaimportuj dane z pliku CSV, aby rozpocząć analizę.")
            if not self.import_in_progress:
                self.import_progress.hide()
                self.lbl_import_status.setText(
                    "Wybierz plik CSV, aby rozpocząć import danych i przygotować analizę."
                )
                if not self.import_log.toPlainText().strip():
                    if self.service.database_exists():
                        self.import_log.setPlainText(
                            "Wybierz plik CSV, aby przygotować aktualny zestaw danych do analizy."
                        )
                    else:
                        self.import_log.setPlainText(
                            "Wybierz plik CSV. Aplikacja automatycznie oczyści dane i zapisze je w tle."
                        )
            return

        self._set_data_tabs_enabled(True)
        lookup_values = self.service.load_lookup_options()
        self.search_filters.set_lookup_options(lookup_values)
        self.group_filters.set_lookup_options(lookup_values)
        self.chart_filters.set_lookup_options(lookup_values)
        self._set_overview_labels(self.service.load_overview_metrics())
        if not self.import_in_progress:
            self.lbl_import_status.setText(
                "Dane są gotowe do analizy. Możesz wybrać nowy plik CSV, aby odświeżyć import."
            )
            if not self.import_log.toPlainText().strip():
                self.import_log.setPlainText(
                    "Dane są dostępne w aplikacji. Aby zaimportować nowy plik, wybierz CSV powyżej."
                )
        if run_default_search:
            self.on_search_records()
            self.on_load_view()
    def on_browse_csv(self) -> None:
        if self.import_in_progress:
            return

        path, _ = QFileDialog.getOpenFileName(
            self,
            "Wybierz plik CSV",
            filter="CSV (*.csv)",
        )
        if path:
            self.le_csv_path.setText(path)
            self.on_import_database()

    def on_import_database(self) -> None:
        csv_path = self.le_csv_path.text().strip()
        if not csv_path:
            self._show_error(
                "Import danych nie powiódł się",
                ValueError("Wskaż plik CSV do importu."),
            )
            return

        if self.import_in_progress:
            return

        self.import_in_progress = True
        self.btn_csv.setEnabled(False)
        self.tabs.setCurrentIndex(0)
        self._set_data_tabs_enabled(False)
        self.import_progress.setRange(0, 0)
        self.import_progress.show()
        self.lbl_import_status.setText(
            "Trwa import danych. Aplikacja automatycznie czyści dane i zapisuje je w tle."
        )
        self.import_log.setPlainText(
            "\n".join(
                [
                    "Import został uruchomiony.",
                    f"Plik CSV: {csv_path}",
                    "Trwa przygotowanie danych i zapis do bazy aplikacji.",
                ]
            )
        )
        self.statusBar().showMessage("Trwa import danych z pliku CSV...", 4000)

        self.import_thread = QThread(self)
        self.import_worker = ImportWorker(self.service, csv_path)
        self.import_worker.moveToThread(self.import_thread)

        # Worker jest tworzony tylko na czas jednego importu i sprzątany po zakończeniu.
        self.import_thread.started.connect(self.import_worker.run)
        self.import_worker.finished.connect(self._on_import_finished)
        self.import_worker.failed.connect(self._on_import_failed)
        self.import_worker.finished.connect(self.import_thread.quit)
        self.import_worker.failed.connect(self.import_thread.quit)
        self.import_worker.finished.connect(self.import_worker.deleteLater)
        self.import_worker.failed.connect(self.import_worker.deleteLater)
        self.import_thread.finished.connect(self.import_thread.deleteLater)
        self.import_thread.finished.connect(self._clear_import_worker)
        self.import_thread.start()

    def _on_import_finished(self, stats, csv_path: str) -> None:
        self.import_in_progress = False
        self.btn_csv.setEnabled(True)
        self.import_progress.hide()
        self.lbl_import_status.setText(
            "Import zakończony. Dane zostały oczyszczone i są gotowe do analizy."
        )
        self.import_log.setPlainText(
            "\n".join(
                [
                    "Import zakończony pomyślnie.",
                    f"Plik CSV: {csv_path}",
                    f"Liczba wierszy wejściowych: {stats.raw_rows}",
                    f"Liczba pacjentów: {stats.patients}",
                    f"Liczba hospitalizacji: {stats.hospital_stays}",
                    f"Liczba pobytów oddziałowych: {stats.unit_stays}",
                    f"Konflikty etniczności pacjentów: {stats.patient_ethnicity_conflicts}",
                    f"Konflikty wieku pacjentów: {stats.patient_age_conflicts}",
                    (
                        "Konflikty rozpoznań na poziomie hospitalizacji: "
                        f"{stats.hospital_diagnosis_conflicts}"
                    ),
                ]
            )
        )
        self.refresh_application_state(run_default_search=True)
        self.tabs.setCurrentIndex(1)
        self.statusBar().showMessage("Dane zostały zaimportowane.", 6000)

    def _on_import_failed(self, message: str) -> None:
        self.import_in_progress = False
        self.btn_csv.setEnabled(True)
        self.import_progress.hide()
        self.lbl_import_status.setText(
            "Import nie został zakończony. Wybierz poprawny plik CSV i spróbuj ponownie."
        )
        self.import_log.setPlainText(
            "\n".join(
                [
                    "Import nie powiódł się.",
                    f"Szczegóły: {message}",
                ]
            )
        )
        self.refresh_application_state(run_default_search=False)
        self._show_error("Import danych nie powiódł się", RuntimeError(message))

    def _clear_import_worker(self) -> None:
        self.import_thread = None
        self.import_worker = None

    def on_search_records(self) -> None:
        try:
            frame = self.service.search_records(self.search_filters.collect_filters())
            self.search_model.set_dataframe(frame)
            self.search_table.resizeColumnsToContents()
            self.statusBar().showMessage(f"Wczytano {len(frame)} rekordów do przeglądu.", 4000)
        except Exception as error:
            self._show_error("Nie udało się wykonać wyszukiwania", error)

    def on_run_group_analysis(self) -> None:
        try:
            analysis_type = self.cb_analysis_type.currentData()
            filters = self.group_filters.collect_filters()

            if analysis_type == "diagnosis_summary":
                frame = self.service.load_diagnosis_summary(
                    filters,
                    limit=self.spin_diagnosis_limit.value(),
                )
                message = "Załadowano statystyki rozpoznań."
            else:
                group_by = self.cb_group_by.currentData()
                frame = self.service.load_group_summary(
                    group_by,
                    filters,
                )
                frame = self._localize_group_frame(frame, group_by)
                message = "Załadowano porównanie grup pacjentów."

            self.group_model.set_dataframe(frame)
            self.group_table.resizeColumnsToContents()
            self.statusBar().showMessage(message, 4000)
        except Exception as error:
            self._show_error("Nie udało się uruchomić analizy", error)

    def on_draw_chart(self) -> None:
        try:
            chart_type = self.cb_chart_type.currentData()
            filters = self.chart_filters.collect_filters()
            if chart_type == "histogram":
                metric = self.cb_hist_metric.currentData()
                frame = self.service.load_histogram_data(metric, filters)
                if frame.empty:
                    raise ValueError("Brak danych dla wybranych filtrów i parametru wykresu.")
                self.chart_canvas.draw_histogram(frame, metric, self.spin_bins.value())
            else:
                group_by = self.cb_chart_group_by.currentData()
                metric = self.cb_group_metric.currentData()
                frame = self.service.load_group_chart_data(group_by, metric, filters)
                if frame.empty:
                    raise ValueError("Brak danych dla wybranych filtrów i ustawień wykresu.")
                frame = self._localize_group_frame(frame, group_by)
                self.chart_canvas.draw_group_bar(
                    frame,
                    metric,
                    self.cb_chart_group_by.currentText(),
                )
            self.statusBar().showMessage("Wykres został odświeżony.", 4000)
        except Exception as error:
            self._show_error("Nie udało się wygenerować wykresu", error)

    def on_load_view(self) -> None:
        try:
            view_name = self.cb_view_name.currentData()
            frame = self.service.load_view_data(view_name, limit=self.spin_view_limit.value())
            self.views_model.set_dataframe(frame)
            self.views_table.resizeColumnsToContents()
            self.statusBar().showMessage(
                f"Wczytano widok: {self.cb_view_name.currentText()}.",
                4000,
            )
        except Exception as error:
            self._show_error("Nie udało się wczytać widoku SQL", error)

    def on_export_search_results(self) -> None:
        self._export_dataframe(self.search_model.dataframe(), "wyniki_wyszukiwania.csv")

    def on_export_group_results(self) -> None:
        self._export_dataframe(self.group_model.dataframe(), "wyniki_analizy_grup.csv")

    def on_export_view_results(self) -> None:
        view_name = self.cb_view_name.currentData() or "widok_sql"
        self._export_dataframe(self.views_model.dataframe(), f"{view_name}.csv")

    def on_reset_search_filters(self) -> None:
        self.search_filters.reset()
        if self.service.database_ready():
            self.search_filters.set_lookup_options(self.service.load_lookup_options())

    def on_reset_group_filters(self) -> None:
        self.group_filters.reset()
        if self.service.database_ready():
            self.group_filters.set_lookup_options(self.service.load_lookup_options())

    def on_reset_chart_filters(self) -> None:
        self.chart_filters.reset()
        if self.service.database_ready():
            self.chart_filters.set_lookup_options(self.service.load_lookup_options())
        self.chart_canvas.draw_placeholder("Wybierz parametry i wygeneruj nowy wykres.")

    def on_reset_view_selection(self) -> None:
        self.cb_view_name.setCurrentIndex(0)
        self.spin_view_limit.setValue(250)
        self.views_model.set_dataframe(pd.DataFrame())
        self._update_view_controls_state()

    def on_export_chart(self) -> None:
        try:
            if not self.chart_canvas.has_chart_data():
                raise ValueError("Brak gotowego wykresu do eksportu.")

            default_path = str(Path.cwd() / "wykres.png")
            path, _ = QFileDialog.getSaveFileName(
                self,
                "Eksportuj wykres do PNG",
                default_path,
                "PNG (*.png)",
            )
            if not path:
                return

            self.chart_canvas.figure.savefig(path, dpi=160, bbox_inches="tight")
            self.statusBar().showMessage(f"Wykres zapisano do pliku: {path}", 5000)
        except Exception as error:
            self._show_error("Nie udało się wyeksportować wykresu", error)

    def _update_group_controls_state(self) -> None:
        is_diagnosis_summary = self.cb_analysis_type.currentData() == "diagnosis_summary"
        self.cb_group_by.setEnabled(not is_diagnosis_summary)
        self.spin_diagnosis_limit.setEnabled(is_diagnosis_summary)
        if is_diagnosis_summary:
            self.lbl_analysis_hint.setText(
                "Analiza zwróci najczęstsze rozpoznania z uwzględnieniem aktualnie ustawionych filtrów."
            )
        else:
            self.lbl_analysis_hint.setText(
                "Analiza porówna grupy pacjentów według wybranego pola i policzy najważniejsze miary agregujące."
            )

    def _update_chart_controls_state(self) -> None:
        is_histogram = self.cb_chart_type.currentData() == "histogram"
        self.cb_hist_metric.setEnabled(is_histogram)
        self.spin_bins.setEnabled(is_histogram)
        self.cb_chart_group_by.setEnabled(not is_histogram)
        self.cb_group_metric.setEnabled(not is_histogram)

    def _update_view_controls_state(self) -> None:
        view_name = self.cb_view_name.currentData()
        self.lbl_view_description.setText(VIEW_DESCRIPTIONS.get(view_name, ""))
        self.spin_view_limit.setEnabled(view_name != "data_quality_summary")

    def _set_overview_labels(self, metrics: dict[str, float | int]) -> None:
        self.lbl_patients.setText(str(metrics.get("patient_count", "-")))
        self.lbl_admissions.setText(str(metrics.get("admission_count", "-")))
        self.lbl_unit_stays.setText(str(metrics.get("care_unit_stay_count", "-")))

        if "unit_mortality_pct" in metrics:
            self.lbl_unit_mortality.setText(f"{metrics['unit_mortality_pct']:.2f}%")
        else:
            self.lbl_unit_mortality.setText("-")

        if "hospital_mortality_pct" in metrics:
            self.lbl_hospital_mortality.setText(f"{metrics['hospital_mortality_pct']:.2f}%")
        else:
            self.lbl_hospital_mortality.setText("-")

    def _set_data_tabs_enabled(self, enabled: bool) -> None:
        for index in range(1, self.tabs.count()):
            self.tabs.setTabEnabled(index, enabled)

    def _create_metric_card(self, title: str) -> tuple[QFrame, QLabel]:
        card = QFrame()
        card.setObjectName("MetricCard")
        layout = QVBoxLayout(card)

        title_label = QLabel(title)
        title_label.setObjectName("MetricTitle")
        value_label = QLabel("-")
        value_label.setObjectName("MetricValue")

        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return card, value_label

    def _build_filter_container(self, filter_panel: FilterPanel, title: str) -> QGroupBox:
        box = QGroupBox(title)
        layout = QVBoxLayout(box)
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(filter_panel)
        layout.addWidget(scroll_area)
        return box

    def _localize_group_frame(self, frame: pd.DataFrame, field_name: str | None) -> pd.DataFrame:
        if field_name is None or "group_value" not in frame.columns:
            return frame

        localized = frame.copy()
        localized["group_value"] = localized["group_value"].map(
            lambda value: get_value_label(field_name, value)
        )
        return localized

    def _configure_table(self, table: QTableView) -> None:
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setSortingEnabled(True)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setStretchLastSection(True)

    @staticmethod
    def _mark_secondary(button: QPushButton) -> None:
        button.setProperty("secondary", True)
        button.style().unpolish(button)
        button.style().polish(button)

    def _export_dataframe(self, frame: pd.DataFrame, suggested_name: str) -> None:
        try:
            if frame.empty:
                raise ValueError("Brak danych do eksportu.")

            default_path = str(Path.cwd() / suggested_name)
            path, _ = QFileDialog.getSaveFileName(
                self,
                "Eksportuj wyniki do pliku CSV",
                default_path,
                "CSV (*.csv)",
            )
            if not path:
                return

            frame.to_csv(path, index=False)
            self.statusBar().showMessage(f"Dane zapisano do pliku: {path}", 5000)
        except Exception as error:
            self._show_error("Nie udało się wyeksportować danych", error)

    def _show_error(self, title: str, error: Exception) -> None:
        QMessageBox.critical(self, title, str(error))
        self.statusBar().showMessage(str(error), 10000)

    def closeEvent(self, event) -> None:  # noqa: N802
        if self.import_in_progress:
            QMessageBox.warning(
                self,
                "Import w toku",
                "Poczekaj na zakończenie importu danych przed zamknięciem aplikacji.",
            )
            event.ignore()
            return

        super().closeEvent(event)

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow {
                background: #f4f7fb;
            }
            QWidget {
                font-size: 13px;
                color: #12314f;
            }
            QLabel {
                color: #12314f;
            }
            QTabWidget {
                color: #12314f;
            }
            QTabWidget::pane {
                border: 1px solid #d9e1eb;
                background: #ffffff;
                border-radius: 10px;
            }
            QTabBar::tab {
                background: #eaf0f7;
                border: 1px solid #d9e1eb;
                border-bottom: none;
                color: #4f6275;
                padding: 10px 16px;
                margin-right: 4px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }
            QTabBar::tab:selected {
                background: #ffffff;
                color: #12314f;
                font-weight: 600;
            }
            QGroupBox {
                background: #ffffff;
                border: 1px solid #d9e1eb;
                border-radius: 10px;
                margin-top: 12px;
                font-weight: 600;
                color: #12314f;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 14px;
                padding: 0 6px;
            }
            QLineEdit, QComboBox, QSpinBox, QPlainTextEdit {
                background: #ffffff;
                color: #12314f;
                border: 1px solid #cfd8e3;
                border-radius: 8px;
                padding: 6px;
                selection-background-color: #cfe2ff;
                selection-color: #12314f;
            }
            QLineEdit[readOnly="true"] {
                background: #eef3f8;
                color: #4f6275;
            }
            QLineEdit::placeholder {
                color: #7e91a5;
            }
            QComboBox QAbstractItemView {
                background: #ffffff;
                color: #12314f;
                border: 1px solid #cfd8e3;
                selection-background-color: #dce6f2;
                selection-color: #12314f;
            }
            QPlainTextEdit {
                background: #fbfdff;
            }
            QProgressBar {
                border: 1px solid #cfd8e3;
                border-radius: 8px;
                background: #eef3f8;
                min-height: 14px;
            }
            QProgressBar::chunk {
                background: #1f6feb;
                border-radius: 8px;
            }
            QScrollArea {
                background: #ffffff;
                border: 1px solid #cfd8e3;
                border-radius: 8px;
            }
            QScrollArea > QWidget > QWidget {
                background: #ffffff;
                color: #12314f;
            }
            QTableView {
                background: #ffffff;
                alternate-background-color: #f5f8fc;
                color: #12314f;
                gridline-color: #d9e1eb;
                border: 1px solid #cfd8e3;
                border-radius: 8px;
                selection-background-color: #d7e7ff;
                selection-color: #12314f;
            }
            QHeaderView::section {
                background: #eaf0f7;
                color: #12314f;
                border: 1px solid #d9e1eb;
                padding: 6px;
                font-weight: 600;
            }
            QPushButton {
                background: #1f6feb;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 8px 14px;
                min-height: 34px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #1558b0;
            }
            QPushButton[secondary="true"] {
                background: #e9eef5;
                color: #12314f;
            }
            QPushButton[secondary="true"]:hover {
                background: #dce6f2;
            }
            QFrame#MetricCard {
                background: #f8fbff;
                border: 1px solid #d9e7f5;
                border-radius: 12px;
                min-width: 180px;
                padding: 8px;
            }
            QLabel#MetricTitle {
                color: #5a6f84;
                font-size: 12px;
                font-weight: 600;
            }
            QLabel#MetricValue {
                color: #12314f;
                font-size: 22px;
                font-weight: 700;
            }
            """
        )
