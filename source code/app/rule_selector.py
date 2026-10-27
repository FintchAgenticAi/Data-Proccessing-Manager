from __future__ import annotations

from typing import Any, Protocol

from app.schemas import RuleDecision


class AgenticRuleSelector(Protocol):
    def select(self, row: dict[str, Any], file_details: dict[str, Any]) -> RuleDecision:
        """Return the clean/quarantine decision for one CSV row."""


class DummyAgenticRuleSelector:
    """Temporary validation rules for demonstrating clean and quarantined rows."""

    def select(self, row: dict[str, Any], file_details: dict[str, Any]) -> RuleDecision:
        if not any(str(value).strip() for value in row.values()):
            return RuleDecision(is_quarantine=True, reason="Row is empty", rule_id="dummy-empty-row")
        if "email" in row and "@" not in str(row["email"]):
            return RuleDecision(
                is_quarantine=True,
                reason="Email is not valid",
                rule_id="dummy-email-format",
            )
        marker_values = {
            str(value).strip().lower()
            for key, value in row.items()
            if key.strip().lower() in {"validation_status", "status", "quarantine"}
        }
        if marker_values & {"invalid", "quarantine", "quarantined", "error", "fail", "failed", "yes", "true"}:
            return RuleDecision(
                is_quarantine=True,
                reason="Temporary validation marker requested quarantine",
                rule_id="temporary-validation-marker",
            )
        return RuleDecision(is_quarantine=False, rule_id="dummy-default-clean")
