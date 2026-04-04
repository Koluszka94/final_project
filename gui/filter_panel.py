from __future__ import annotations

"""Wspólny formularz filtrów używany w zakładkach wyszukiwania, analiz i wykresów."""

from typing import Iterable

from PyQt6.QtGui import QIntValidator
from PyQt6.QtWidgets import QComboBox, QFormLayout, QGroupBox, QLineEdit, QVBoxLayout, QWidget

from gui.labels import get_value_label
from queries.patient_queries import SearchFilters


class FilterPanel(QWidget):
    """Zbiera kryteria filtrowania i zamienia je na obiekt SearchFilters."""

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout(self)

        self.le_patient_id = QLineEdit()
        self.le_admission_id = QLineEdit()
        self.le_care_unit_stay_id = QLineEdit()
        self.cb_gender = QComboBox()
        self.cb_ethnicity = QComboBox()
        self.le_age_min = QLineEdit()
        self.le_age_max = QLineEdit()
        self.cb_age_group = QComboBox()
        self.le_hospital_id = QLineEdit()
        self.le_ward_id = QLineEdit()
        self.cb_unit_type = QComboBox()
        self.cb_unit_stay_type = QComboBox()
        self.cb_hospital_status = QComboBox()
        self.cb_unit_status = QComboBox()
        self.le_diagnosis = QLineEdit()
        self.le_limit = QLineEdit("500")

        self._apply_validators()
        self._apply_placeholders()
        self._apply_tooltips()

        layout.addWidget(self._build_identifiers_box())
        layout.addWidget(self._build_demographics_box())
        layout.addWidget(self._build_clinical_box())
        layout.addWidget(self._build_scope_box())
        layout.addStretch()

        self.reset()

    def _build_identifiers_box(self) -> QGroupBox:
        """Buduje sekcję z identyfikatorami pacjenta i pobytu."""
        box = QGroupBox("Identyfikatory")
        form = QFormLayout(box)
        form.addRow("Pacjent", self.le_patient_id)
        form.addRow("Hospitalizacja", self.le_admission_id)
        form.addRow("Pobyt oddziałowy", self.le_care_unit_stay_id)
        return box

    def _build_demographics_box(self) -> QGroupBox:
        """Buduje sekcję z filtrami demograficznymi."""
        box = QGroupBox("Dane pacjenta")
        form = QFormLayout(box)
        form.addRow("Płeć", self.cb_gender)
        form.addRow("Pochodzenie etniczne", self.cb_ethnicity)
        form.addRow("Grupa wiekowa", self.cb_age_group)
        form.addRow("Wiek od", self.le_age_min)
        form.addRow("Wiek do", self.le_age_max)
        return box

    def _build_clinical_box(self) -> QGroupBox:
        """Buduje sekcję z filtrami klinicznymi i organizacyjnymi."""
        box = QGroupBox("Dane kliniczne")
        form = QFormLayout(box)
        form.addRow("Szpital", self.le_hospital_id)
        form.addRow("Kod oddziału", self.le_ward_id)
        form.addRow("Typ oddziału", self.cb_unit_type)
        form.addRow("Typ pobytu oddziałowego", self.cb_unit_stay_type)
        form.addRow("Status wypisu ze szpitala", self.cb_hospital_status)
        form.addRow("Status wypisu z oddziału", self.cb_unit_status)
        form.addRow("Rozpoznanie zawiera", self.le_diagnosis)
        return box

    def _build_scope_box(self) -> QGroupBox:
        """Buduje sekcję sterującą zakresem wyników."""
        box = QGroupBox("Zakres wyników")
        form = QFormLayout(box)
        form.addRow("Limit rekordów", self.le_limit)
        return box

    def _apply_validators(self) -> None:
        """Ogranicza wybrane pola do poprawnych zakresów liczbowych."""
        positive_id_validator = QIntValidator(0, 2_147_483_647, self)
        age_validator = QIntValidator(0, 150, self)
        limit_validator = QIntValidator(1, 5000, self)

        for widget in [
            self.le_admission_id,
            self.le_care_unit_stay_id,
            self.le_hospital_id,
            self.le_ward_id,
        ]:
            widget.setValidator(positive_id_validator)

        self.le_age_min.setValidator(age_validator)
        self.le_age_max.setValidator(age_validator)
        self.le_limit.setValidator(limit_validator)

    def _apply_placeholders(self) -> None:
        """Ustawia przykłady podpowiadające oczekiwany format danych."""
        self.le_patient_id.setPlaceholderText("np. 002-15638")
        self.le_admission_id.setPlaceholderText("liczba całkowita")
        self.le_care_unit_stay_id.setPlaceholderText("liczba całkowita")
        self.le_age_min.setPlaceholderText("minimalny wiek")
        self.le_age_max.setPlaceholderText("maksymalny wiek")
        self.le_hospital_id.setPlaceholderText("liczba całkowita")
        self.le_ward_id.setPlaceholderText("liczba całkowita")
        self.le_diagnosis.setPlaceholderText("fragment tekstu")
        self.le_limit.setPlaceholderText("1-5000")

    def _apply_tooltips(self) -> None:
        """Dodaje krótkie objaśnienia do mniej oczywistych pól."""
        self.le_ward_id.setToolTip("Numeryczny kod oddziału z oryginalnego zbioru danych.")
        self.cb_unit_type.setToolTip(
            "Lista pokazuje rozwinięte, polskie opisy typów oddziałów zamiast samych skrótów."
        )
        self.cb_unit_stay_type.setToolTip(
            "Opis sposobu pobytu na oddziale, np. przyjęcie, ponowne przyjęcie albo transfer."
        )

    def set_lookup_options(self, lookup_values: dict[str, Iterable[str]]) -> None:
        """Uzupełnia listy rozwijane unikalnymi wartościami z SQLite."""
        combo_map = {
            "gender": self.cb_gender,
            "ethnicity": self.cb_ethnicity,
            "age_group": self.cb_age_group,
            "unit_type": self.cb_unit_type,
            "unit_stay_type": self.cb_unit_stay_type,
            "hospital_discharge_status": self.cb_hospital_status,
            "unit_discharge_status": self.cb_unit_status,
        }

        for field_name, combo in combo_map.items():
            current_value = combo.currentData()
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("(Dowolne)", None)
            for value in lookup_values.get(field_name, []):
                combo.addItem(get_value_label(field_name, value), str(value))
            target_index = 0
            for index in range(combo.count()):
                if combo.itemData(index) == current_value:
                    target_index = index
                    break
            combo.setCurrentIndex(target_index)
            combo.blockSignals(False)

    def collect_filters(self) -> SearchFilters:
        """Zamienia aktualny stan formularza na niemutowalny obiekt filtrów."""
        return SearchFilters(
            patient_id=self.le_patient_id.text().strip() or None,
            admission_id=self._get_optional_int(self.le_admission_id),
            care_unit_stay_id=self._get_optional_int(self.le_care_unit_stay_id),
            gender=self._combo_value(self.cb_gender),
            ethnicity=self._combo_value(self.cb_ethnicity),
            age_min=self._get_optional_int(self.le_age_min),
            age_max=self._get_optional_int(self.le_age_max),
            age_group=self._combo_value(self.cb_age_group),
            hospital_id=self._get_optional_int(self.le_hospital_id),
            ward_id=self._get_optional_int(self.le_ward_id),
            unit_type=self._combo_value(self.cb_unit_type),
            unit_stay_type=self._combo_value(self.cb_unit_stay_type),
            hospital_discharge_status=self._combo_value(self.cb_hospital_status),
            unit_discharge_status=self._combo_value(self.cb_unit_status),
            diagnosis_contains=self.le_diagnosis.text().strip() or None,
            limit=self._get_optional_int(self.le_limit) or 500,
        )

    def reset(self) -> None:
        """Czyści wszystkie filtry i przywraca domyślne wartości interfejsu."""
        for widget in [
            self.le_patient_id,
            self.le_admission_id,
            self.le_care_unit_stay_id,
            self.le_age_min,
            self.le_age_max,
            self.le_hospital_id,
            self.le_ward_id,
            self.le_diagnosis,
        ]:
            widget.clear()

        self.le_limit.setText("500")

        for combo in [
            self.cb_gender,
            self.cb_ethnicity,
            self.cb_age_group,
            self.cb_unit_type,
            self.cb_unit_stay_type,
            self.cb_hospital_status,
            self.cb_unit_status,
        ]:
            combo.clear()
            combo.addItem("(Dowolne)", None)

    @staticmethod
    def _get_optional_int(widget: QLineEdit) -> int | None:
        text = widget.text().strip()
        return None if not text else int(text)

    @staticmethod
    def _combo_value(widget: QComboBox) -> str | None:
        value = widget.currentData()
        return None if value is None else str(value)
