from __future__ import annotations

"""Punkt startowy aplikacji desktopowej."""

import sys

from PyQt6.QtWidgets import QApplication, QStyleFactory

from gui.main_window import MainWindow


def main() -> None:
    """Tworzy aplikację Qt i uruchamia główne okno."""
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion"))
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
