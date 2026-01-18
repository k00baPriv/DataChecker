# datachecker/types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


Record = Mapping[str, Any]


class ValidationPlan(Protocol):
    """Marker protocol for compiled validation plans (PydanticPlan, PanderaPlan, etc.)."""

    ...


@dataclass(frozen=True)
class RowError:
    row_index: int
    record_hint: dict[str, Any]
    errors: list[dict[str, Any]]


@dataclass(frozen=True)
class ValidationReport:
    schema_name: str

    # For list[Record] batches, total/valid/invalid are known.
    # For Spark DataFrames, total/valid may be None unless you trigger expensive counts.
    total: int | None
    valid: int | None
    invalid: int | None

    # Optional details & samples (works for both pydantic and pandera)
    row_errors: list[RowError] | None = None
    sample_failures: list[dict[str, Any]] | None = None
    details: dict[str, Any] | None = None

    def ok(self) -> bool:
        return (self.invalid or 0) == 0


class BatchValidator(Protocol):
    """
    Validate a batch (could be list[Record], pandas.DataFrame, pyspark.sql.DataFrame, etc.)
    against a compiled plan.
    """

    def validate_batch(self, plan: ValidationPlan, batch: Any) -> ValidationReport: ...
