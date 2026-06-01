"""Unit tests for statistical tests and anomaly detection."""
from __future__ import annotations

import numpy as np
import pytest


def test_ks_test_identical():
    """KS test on identical distributions should not be significant."""
    from drift_detection.statistical_tests import ks_test
    values = list(np.random.normal(0, 1, 300))
    result = ks_test(values, values)
    assert not result.significant, "Identical distributions should not be significant"
    assert result.p_value > 0.05


def test_ks_test_different():
    """KS test on very different distributions should be significant."""
    from drift_detection.statistical_tests import ks_test
    a = list(np.random.normal(0, 1, 300))
    b = list(np.random.normal(10, 1, 300))
    result = ks_test(a, b)
    assert result.significant, "Very different distributions should be significant"
    assert result.p_value < 0.05


def test_z_score_mean_value():
    """Z-score of exactly the mean should be 0."""
    from drift_detection.statistical_tests import z_score_test
    baseline = [float(x) for x in range(1, 101)]  # mean=50.5
    current_at_mean = [50.5] * 100
    result = z_score_test(baseline, current_at_mean)
    assert result.z_score < 0.1, f"Z-score should be ~0 for mean value, got {result.z_score}"


def test_z_score_outlier():
    """Z-score should be high for obvious outlier."""
    from drift_detection.statistical_tests import z_score_test
    baseline = [10.0] * 100  # mean=10, std≈0
    current_outlier = [1000.0] * 100  # far from baseline
    result = z_score_test(baseline, current_outlier)
    assert result.z_score > 3.0 or result.is_anomalous


def test_kl_divergence_zero_identical():
    """KL divergence of identical distributions should be ~0."""
    from drift_detection.statistical_tests import kl_divergence
    values = list(np.random.normal(0, 1, 300))
    kl = kl_divergence(values, values)
    assert kl < 0.1, f"KL divergence of identical distributions should be ~0, got {kl}"


def test_kl_divergence_different():
    """KL divergence of different distributions should be positive."""
    from drift_detection.statistical_tests import kl_divergence
    a = list(np.random.normal(0, 1, 300))
    b = list(np.random.normal(5, 1, 300))
    kl = kl_divergence(a, b)
    assert kl > 0.0
