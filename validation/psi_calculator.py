"""Population Stability Index (PSI) calculation."""
from __future__ import annotations

import numpy as np

EPSILON = 1e-6
N_BINS = 10


def compute_psi(expected: list[float], actual: list[float], n_bins: int = N_BINS) -> float:
    """
    PSI = Σ (actual% - expected%) × ln(actual% / expected%)

    Thresholds:
      < 0.1  → no significant shift (green)
      0.1–0.2 → moderate shift (yellow) — monitor
      > 0.2  → major shift (red) — alert

    Both arrays are raw feature values (not pre-binned).
    """
    if not expected or not actual:
        return 0.0

    expected_arr = np.array(expected, dtype=float)
    actual_arr = np.array(actual, dtype=float)

    # Determine bins from expected distribution
    min_val = float(np.min(expected_arr))
    max_val = float(np.max(expected_arr))

    if min_val == max_val:
        return 0.0

    bins = np.linspace(min_val, max_val, n_bins + 1)
    bins[-1] += EPSILON  # include max value in last bin

    expected_counts, _ = np.histogram(expected_arr, bins=bins)
    actual_counts, _ = np.histogram(actual_arr, bins=bins)

    expected_pct = expected_counts / (len(expected_arr) + EPSILON)
    actual_pct = actual_counts / (len(actual_arr) + EPSILON)

    # Replace zeros to avoid log(0)
    expected_pct = np.where(expected_pct == 0, EPSILON, expected_pct)
    actual_pct = np.where(actual_pct == 0, EPSILON, actual_pct)

    psi = float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))
    return round(max(0.0, psi), 6)


def psi_severity(psi: float, warn: float = 0.1, alert: float = 0.2) -> str:
    if psi < warn:
        return "ok"
    if psi < alert:
        return "warn"
    return "alert"
