"""Narzędzia analityczne SQL używane przez analizy grup i wykresy."""

from .group_comparison import (
    BAR_METRICS,
    GROUP_BY_FIELDS,
    HISTOGRAM_METRICS,
    fetch_diagnosis_summary,
    fetch_group_comparison,
    fetch_group_metric_bar,
    fetch_histogram_series,
)

__all__ = [
    "BAR_METRICS",
    "GROUP_BY_FIELDS",
    "HISTOGRAM_METRICS",
    "fetch_diagnosis_summary",
    "fetch_group_comparison",
    "fetch_group_metric_bar",
    "fetch_histogram_series",
]
