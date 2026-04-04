from __future__ import annotations

"""Model tabeli Qt wyświetlający obiekty pandas DataFrame w interfejsie."""

import pandas as pd
from PyQt6.QtCore import QAbstractTableModel, Qt, QVariant

from gui.labels import get_column_label, get_value_label


class PandasTableModel(QAbstractTableModel):
    """Udostępnia DataFrame przez interfejs QAbstractTableModel."""

    def __init__(self, df: pd.DataFrame | None = None, parent=None):
        super().__init__(parent)
        self._df = df if df is not None else pd.DataFrame()

    def set_dataframe(self, df: pd.DataFrame) -> None:
        """Podmienia aktualny zbiór danych i powiadamia widok Qt."""
        self.beginResetModel()
        self._df = df
        self.endResetModel()

    def dataframe(self) -> pd.DataFrame:
        """Zwraca kopię, aby kod wywołujący nie modyfikował stanu modelu."""
        return self._df.copy()

    def rowCount(self, parent=None):  # noqa: N802
        return 0 if self._df is None else len(self._df.index)

    def columnCount(self, parent=None):  # noqa: N802
        return 0 if self._df is None else len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
            return QVariant()

        column_name = self._df.columns[index.column()]
        value = self._df.iat[index.row(), index.column()]
        if pd.isna(value):
            return ""
        if isinstance(value, float):
            formatted = f"{value:.2f}" if not value.is_integer() else str(int(value))
            return get_value_label(column_name, formatted)
        return get_value_label(column_name, value)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):  # noqa: N802
        if role != Qt.ItemDataRole.DisplayRole or self._df is None:
            return QVariant()

        if orientation == Qt.Orientation.Horizontal:
            return (
                get_column_label(str(self._df.columns[section]))
                if section < len(self._df.columns)
                else QVariant()
            )

        return str(self._df.index[section]) if section < len(self._df.index) else QVariant()
