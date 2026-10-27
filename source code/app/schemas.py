from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ProcessingStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class QuarantineStatus(str, Enum):
    quarantined = "quarantined"
    confirmed = "confirmed"
    released = "released"


class ReviewDecision(str, Enum):
    keep_quarantined = "keep_quarantined"
    release = "release"


class ProcessingJobRequest(BaseModel):
    model_config = {
        "json_schema_extra": {
            "example": {
                "csv_filename": "2026_batch-a.csv",
                "supplier_name": "Supplier A",
                "country": "US",
            }
        }
    }

    csv_filename: str = Field(..., min_length=1, description="CSV filename returned by File Manager.")
    data_batch_id: int | None = Field(
        default=None,
        ge=1,
        description="Optional existing batch ID. If omitted, Data Processing Manager generates it.",
    )
    supplier_name: str = Field(..., min_length=1)
    country: str = Field(..., min_length=1)


class RuleDecision(BaseModel):
    is_quarantine: bool
    reason: str | None = None
    rule_id: str = "agentic-default"


class ProcessingJobResponse(BaseModel):
    data_batch_id: int
    csv_filename: str
    processed_count: int
    processed_attempt: int
    clean_count: int
    quarantine_count: int


class QuarantineItem(BaseModel):
    quarantine_id: int
    raw_record_id: int
    status: QuarantineStatus
    reason: str
    row_number: int
    data: dict[str, Any]


class ReviewRequest(BaseModel):
    decision: ReviewDecision
    reason: str | None = None


class ReuploadResponse(BaseModel):
    message: str
    new_filename: str
    version: int
    data_batch_id: int


class PipelineRequest(BaseModel):
    rule_selector_path: str = Field("/rules/select", description="Agentic rule manager selection endpoint.")
    transformation_path: str = Field("/transform", description="Transformation service endpoint.")
    validation_path: str = Field("/validate", description="Validation service endpoint.")
    deduplication_path: str = Field("/deduplicate", description="Deduplication service endpoint.")


class PipelineResponse(BaseModel):
    pipeline_execution_id: int
    data_batch_id: int
    status: str
    processed_count: int
    clean_count: int
    quarantine_count: int
