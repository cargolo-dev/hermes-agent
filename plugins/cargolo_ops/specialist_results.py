"""Common specialist worker result contract for CARGOLO Coordinator flows."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models import utc_now_iso


class SpecialistStatus(str, Enum):
    OK = "ok"
    NEEDS_REVIEW = "needs_review"
    FAILED = "failed"
    SKIPPED = "skipped"


class SpecialistResult(BaseModel):
    """Uniform result shape for Python, Hermes, and Codex-backed specialists."""

    model_config = ConfigDict(extra="allow", use_enum_values=False)

    agent: str
    status: SpecialistStatus
    confidence: float | None = None
    summary: str = ""
    findings: list[dict[str, Any]] = Field(default_factory=list)
    risks: list[dict[str, Any]] = Field(default_factory=list)
    recommended_actions: list[dict[str, Any]] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    requires_human: bool = False
    write_intents: list[dict[str, Any]] = Field(default_factory=list)
    created_at: str = Field(default_factory=utc_now_iso)

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, value: float | None) -> float | None:
        if value is None:
            return None
        return max(0.0, min(1.0, float(value)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "status": self.status.value,
            "confidence": self.confidence,
            "summary": self.summary,
            "findings": self.findings,
            "risks": self.risks,
            "recommended_actions": self.recommended_actions,
            "evidence_refs": self.evidence_refs,
            "requires_human": self.requires_human,
            "write_intents": self.write_intents,
            "created_at": self.created_at,
        }
