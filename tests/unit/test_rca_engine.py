"""Unit tests for the LLM RCA engine."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from diagnostics.schemas import DriftReport


def make_drift_report(severity: str = "alert", psi: float = 0.25) -> DriftReport:
    return DriftReport(
        feature_name="avg_spend_1h",
        psi_score=psi,
        ks_statistic=0.15,
        ks_p_value=0.02,
        z_score=3.8,
        severity=severity,
        baseline_period="last_7d",
        current_period="last_1h",
        sample_size=500,
    )


@pytest.mark.asyncio
async def test_fallback_used_when_api_key_missing():
    """RCA engine falls back to rule-based when no API key."""
    import os
    os.environ["ANTHROPIC_API_KEY"] = ""

    from diagnostics.rca_engine import RCAEngine
    engine = RCAEngine()
    report = make_drift_report()
    diagnosis = await engine.analyze(report)

    assert diagnosis.feature_name == "avg_spend_1h"
    assert diagnosis.probable_cause
    assert diagnosis.confidence > 0
    assert len(diagnosis.remediation_steps) > 0
    assert diagnosis.fallback_used is True


def test_rule_based_high_psi_diagnosis():
    """Rule-based fallback returns 'data_pipeline_delay' for high PSI + high Z."""
    from diagnostics.rca_engine import RCAEngine
    engine = RCAEngine()
    report = make_drift_report(psi=0.30)
    report.z_score = 4.0

    diagnosis = engine._rule_based_fallback(report)
    assert diagnosis.cause_category == "data_pipeline_delay"
    assert diagnosis.urgency in ("high", "critical")
    assert diagnosis.confidence > 0.5


def test_rule_based_low_psi_diagnosis():
    """Rule-based fallback returns 'seasonal_pattern' for low PSI with KS significance."""
    from diagnostics.rca_engine import RCAEngine
    engine = RCAEngine()
    report = make_drift_report(psi=0.05, severity="warn")
    report.ks_p_value = 0.03
    report.z_score = 1.0

    diagnosis = engine._rule_based_fallback(report)
    assert diagnosis.cause_category in ("seasonal_pattern", "unknown")


@pytest.mark.asyncio
async def test_llm_rca_parses_tool_use():
    """When Claude returns a valid tool_use response, it parses into RCADiagnosis."""
    import os
    os.environ["ANTHROPIC_API_KEY"] = "sk-ant-fake"

    mock_block = MagicMock()
    mock_block.type = "tool_use"
    mock_block.name = "submit_rca_diagnosis"
    mock_block.input = {
        "probable_cause": "Kafka consumer lag caused stale features",
        "cause_category": "data_pipeline_delay",
        "confidence": 0.87,
        "affected_features": ["avg_spend_1h"],
        "remediation_steps": ["Check Kafka consumer lag", "Verify Redis TTL"],
        "urgency": "high",
        "estimated_impact": "Model accuracy degradation ~2%",
    }

    mock_usage = MagicMock()
    mock_usage.input_tokens = 500
    mock_usage.output_tokens = 200

    mock_response = MagicMock()
    mock_response.content = [mock_block]
    mock_response.usage = mock_usage

    mock_client = AsyncMock()
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    from diagnostics.rca_engine import RCAEngine
    engine = RCAEngine()
    engine._client = mock_client

    report = make_drift_report()
    diagnosis = await engine._analyze_with_llm(report)

    assert diagnosis.cause_category == "data_pipeline_delay"
    assert diagnosis.confidence == 0.87
    assert diagnosis.urgency == "high"
    assert diagnosis.tokens_used == 700
    assert not diagnosis.fallback_used
