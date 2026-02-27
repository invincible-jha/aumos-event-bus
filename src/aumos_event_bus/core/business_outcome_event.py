"""BusinessOutcomeEvent — Pydantic message schema for business outcome events.

This module defines the canonical Python event schema for business outcomes
that are associated with AI decisions. Published to Kafka and consumed by
aumos-ai-finops for cost-to-outcome attribution (ROI calculation).

Kafka topic: finops.business_outcomes
Schema subject: finops.business_outcomes-value

Field semantics:
  decision_id          — UUID of the AI decision this outcome is attributed to
  tenant_id            — Tenant that owns the decision and outcome
  outcome_type         — Category of outcome (e.g., revenue, cost_saved, risk_avoided)
  outcome_value_usd    — Monetary value of the outcome in USD (None if not yet monetized)
  outcome_value_raw    — Raw numeric value before monetization (e.g., hours, errors)
  outcome_unit         — Unit for outcome_value_raw (e.g., hours, incidents, conversions)
  ai_system_id         — Identifier of the AI system that produced the decision
  use_case             — Business use case label (e.g., fraud_detection, claims_triage)
  outcome_timestamp_ms — Unix epoch milliseconds when the outcome was realized
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


OutcomeType = Literal[
    "revenue_generated",
    "cost_saved",
    "risk_avoided",
    "error_prevented",
    "time_saved",
    "conversion",
    "churn_prevented",
    "fraud_blocked",
    "compliance_upheld",
    "custom",
]


class BusinessOutcomeEvent(BaseModel):
    """Canonical Pydantic schema for a business outcome event.

    Represents the realized business value of a single AI decision.
    Consumed by aumos-ai-finops to complete cost-to-outcome attribution.

    Attributes:
        decision_id: UUID of the AI decision that produced this outcome.
        tenant_id: Tenant that owns this decision and outcome.
        outcome_type: Categorical type of business outcome.
        outcome_value_usd: Monetized USD value (None if not yet monetized).
        outcome_value_raw: Raw numeric metric before monetization.
        outcome_unit: Unit of measurement for outcome_value_raw.
        ai_system_id: Identifier of the AI system that made the decision.
        use_case: Business use case label for grouping and reporting.
        outcome_timestamp_ms: Unix epoch milliseconds when outcome was realized.
        correlation_id: Optional tracing correlation ID.
        metadata: Arbitrary key-value metadata for extensibility.
    """

    decision_id: str = Field(
        ...,
        description="UUID of the AI decision this outcome is attributed to",
    )
    tenant_id: str = Field(
        ...,
        description="Tenant that owns the decision and outcome",
    )
    outcome_type: OutcomeType = Field(
        ...,
        description="Categorical type of business outcome",
    )
    outcome_value_usd: Decimal | None = Field(
        default=None,
        ge=Decimal("0"),
        decimal_places=4,
        description="Monetized outcome value in USD (None if value not yet known)",
    )
    outcome_value_raw: float | None = Field(
        default=None,
        description="Raw numeric value before monetization (e.g., 2.5 hours, 1 incident)",
    )
    outcome_unit: str | None = Field(
        default=None,
        max_length=100,
        description="Unit for outcome_value_raw (e.g., hours, incidents, conversions, usd)",
    )
    ai_system_id: str = Field(
        ...,
        max_length=255,
        description="Identifier of the AI system that produced the decision",
    )
    use_case: str = Field(
        ...,
        max_length=255,
        description="Business use case label (e.g., fraud_detection, claims_triage)",
    )
    outcome_timestamp_ms: int = Field(
        ...,
        gt=0,
        description="Unix epoch milliseconds when the outcome was realized",
    )
    correlation_id: str | None = Field(
        default=None,
        max_length=255,
        description="Optional tracing correlation ID for cross-service observability",
    )
    metadata: dict = Field(
        default_factory=dict,
        description="Arbitrary key-value metadata for extensibility",
    )

    model_config = {"frozen": True}


__all__ = ["BusinessOutcomeEvent", "OutcomeType"]
