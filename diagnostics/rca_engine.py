"""
LLM-based Root Cause Analysis engine.

Uses Claude API with:
- Tool use for structured output (no fragile JSON parsing)
- Prompt caching on the system prompt (saves tokens on repeated calls)
- Rule-based fallback if API is unavailable
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import config
from .schemas import DriftReport, RCADiagnosis
from .context_builder import build_rca_context
from .prompt_templates import SYSTEM_PROMPT, RCA_TOOL_DEFINITION

logger = logging.getLogger(__name__)


class RCAEngine:
    def __init__(self) -> None:
        self._client: Optional[anthropic.AsyncAnthropic] = None

    def _get_client(self) -> anthropic.AsyncAnthropic:
        if self._client is None:
            if not config.anthropic_api_key:
                raise ValueError("ANTHROPIC_API_KEY not configured")
            self._client = anthropic.AsyncAnthropic(api_key=config.anthropic_api_key)
        return self._client

    async def analyze(
        self,
        report: DriftReport,
        feature_metadata: dict | None = None,
        db=None,
    ) -> RCADiagnosis:
        try:
            return await self._analyze_with_llm(report, feature_metadata)
        except Exception as exc:
            logger.warning("LLM RCA failed (%s), using rule-based fallback", exc)
            return self._rule_based_fallback(report)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(anthropic.APIConnectionError),
        reraise=False,
    )
    async def _analyze_with_llm(
        self,
        report: DriftReport,
        feature_metadata: dict | None = None,
    ) -> RCADiagnosis:
        client = self._get_client()
        context = build_rca_context(report, feature_metadata)

        response = await client.messages.create(
            model=config.anthropic_model,
            max_tokens=config.rca_max_tokens,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},  # Cache the large system prompt
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": f"Analyze this feature drift report and diagnose the root cause:\n\n{context}",
                }
            ],
            tools=[RCA_TOOL_DEFINITION],
            tool_choice={"type": "tool", "name": "submit_rca_diagnosis"},
        )

        # Extract tool use result
        tool_result = None
        for block in response.content:
            if block.type == "tool_use" and block.name == "submit_rca_diagnosis":
                tool_result = block.input
                break

        if not tool_result:
            raise ValueError("No tool use block in response")

        tokens_used = response.usage.input_tokens + response.usage.output_tokens

        return RCADiagnosis(
            feature_name=report.feature_name,
            probable_cause=tool_result["probable_cause"],
            cause_category=tool_result["cause_category"],
            confidence=float(tool_result["confidence"]),
            affected_features=tool_result["affected_features"],
            remediation_steps=tool_result["remediation_steps"],
            urgency=tool_result["urgency"],
            estimated_impact=tool_result["estimated_impact"],
            model_used=config.anthropic_model,
            tokens_used=tokens_used,
            fallback_used=False,
        )

    def _rule_based_fallback(self, report: DriftReport) -> RCADiagnosis:
        """Rule-based diagnosis when Claude API is unavailable."""
        feature = report.feature_name
        psi = report.psi_score
        z = abs(report.z_score)
        ks_sig = report.ks_p_value < 0.05

        if psi > 0.2 and z > 3.0:
            cause = "data_pipeline_delay"
            probable = (
                f"Major distribution shift in {feature} (PSI={psi:.3f}, Z={z:.2f}). "
                "Likely cause: Kafka consumer lag causing stale cached values or upstream data volume spike."
            )
            steps = [
                f"1. Check Kafka consumer lag for topic ecommerce.events.raw (group: feature-processor-cg)",
                f"2. Verify Redis TTL for features:{feature[:10]}:* keys",
                f"3. Check Spark streaming micro-batch processing time in Spark UI",
                f"4. Review ingestion service logs for errors in the last hour",
            ]
            urgency = "high"
            confidence = 0.72
        elif psi > 0.1:
            cause = "upstream_data_quality"
            probable = (
                f"Moderate distribution shift in {feature} (PSI={psi:.3f}). "
                "Possible upstream data quality issue or legitimate seasonal pattern."
            )
            steps = [
                f"1. Review event schema changes in ingestion service",
                f"2. Check null/invalid transaction_amount rates in raw events",
                f"3. Compare event volume to same time last week",
                f"4. Verify no recent schema migrations on upstream services",
            ]
            urgency = "medium"
            confidence = 0.55
        elif ks_sig:
            cause = "seasonal_pattern"
            probable = (
                f"Statistically significant distribution change in {feature} "
                f"(KS p-value={report.ks_p_value:.4f}). Could be legitimate seasonal variation."
            )
            steps = [
                f"1. Compare current distribution to same hour/day last week",
                f"2. Check for marketing campaigns or external events",
                f"3. Update baseline distribution if change is expected",
            ]
            urgency = "low"
            confidence = 0.45
        else:
            cause = "unknown"
            probable = f"Minor drift detected in {feature}. Insufficient signal for confident diagnosis."
            steps = ["1. Continue monitoring", "2. Collect more data before taking action"]
            urgency = "low"
            confidence = 0.30

        return RCADiagnosis(
            feature_name=feature,
            probable_cause=probable,
            cause_category=cause,
            confidence=confidence,
            affected_features=[feature],
            remediation_steps=steps,
            urgency=urgency,
            estimated_impact=f"Potential model accuracy degradation for features depending on {feature}.",
            model_used="rule_based_fallback",
            fallback_used=True,
        )


# Module-level singleton
rca_engine = RCAEngine()
