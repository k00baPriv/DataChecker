from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, cast

from pydantic import ValidationError

from datachecker.plans import PydanticPlan


# ---- Common types ----
Record = dict[
    str, Any
]  # your sources yield Mapping[str, Any]; normalize to dict for pydantic


# ---- Batch validation interfaces ----
class ValidationPlan(Protocol): ...


@dataclass(frozen=True)
class RowError:
    row_index: int
    record_hint: dict[str, Any]
    errors: list[dict[str, Any]]


@dataclass(frozen=True)
class ValidationReport:
    schema_name: str
    total: int
    valid: int
    invalid: int
    row_errors: list[RowError]

    def ok(self) -> bool:
        return self.invalid == 0


class BatchValidator(Protocol):
    def validate_batch(
        self, plan: ValidationPlan, records: list[Record]
    ) -> ValidationReport: ...


# ---- Concrete BatchValidator implementation ----
@dataclass(frozen=True)
class PydanticBatchValidator(BatchValidator):
    """
    Validates each record in the batch using plan.model.model_validate(...).
    - returns a ValidationReport (never raises due to validation errors)
    - collects per-row Pydantic errors
    """

    record_hint_keys: tuple[str, ...] = ("id", "email")
    max_errors: int | None = None

    # ✅ IMPORTANT: signature must match the Protocol (ValidationPlan), not PydanticPlan
    def validate_batch(
        self, plan: ValidationPlan, records: list[Record]
    ) -> ValidationReport:
        # Narrow internally
        if not isinstance(plan, PydanticPlan):
            raise TypeError(
                f"PydanticBatchValidator expects PydanticPlan, got {type(plan).__name__}"
            )
        pplan = plan

        row_errors: list[RowError] = []
        valid = 0

        for i, rec in enumerate(records):
            rec_dict = dict(rec)
            try:
                pplan.model.model_validate(rec_dict)
                valid += 1
            except ValidationError as e:
                hint = {
                    k: rec_dict.get(k) for k in self.record_hint_keys if k in rec_dict
                }
                row_errors.append(
                    RowError(
                        row_index=i,
                        record_hint=hint,
                        errors=cast(list[dict[str, Any]], e.errors()),
                    )
                )
                if self.max_errors is not None and len(row_errors) >= self.max_errors:
                    break

        total_checked = (
            len(records)
            if self.max_errors is None
            else min(len(records), valid + len(row_errors))
        )
        invalid = total_checked - valid

        return ValidationReport(
            schema_name=pplan.name,
            total=total_checked,
            valid=valid,
            invalid=invalid,
            row_errors=row_errors,
        )
