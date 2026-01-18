# datachecker/batch_validators.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import ValidationError

from pandera.errors import SchemaErrors

from datachecker.plans import PydanticPlan, PanderaPlan
from datachecker.types import BatchValidator, RowError, ValidationPlan, ValidationReport


# ---- Pydantic (records: list[dict]) ----


@dataclass(frozen=True)
class PydanticBatchValidator(BatchValidator):
    """
    Validates each record in the batch using plan.model.model_validate(...).
    - returns a ValidationReport (never raises due to validation errors)
    - collects per-row Pydantic errors
    """

    record_hint_keys: tuple[str, ...] = ("id", "email")
    max_errors: int | None = None

    def validate_batch(self, plan: ValidationPlan, batch: Any) -> ValidationReport:
        if not isinstance(plan, PydanticPlan):
            raise TypeError(
                f"PydanticBatchValidator expects PydanticPlan, got {type(plan).__name__}"
            )
        records = cast(list[dict[str, Any]], batch)

        row_errors: list[RowError] = []
        valid = 0

        for i, rec in enumerate(records):
            rec_dict = dict(rec)
            try:
                plan.model.model_validate(rec_dict)
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
            schema_name=plan.name,
            total=total_checked,
            valid=valid,
            invalid=invalid,
            row_errors=row_errors,
            sample_failures=None,
            details={"validator": "pydantic"},
        )


# ---- Pandera (Spark DataFrame) ----

# ---- Pandera (Spark DataFrame) ----


@dataclass(frozen=True)
class PanderaBatchValidator(BatchValidator):
    """
    Validates a Spark DataFrame using Pandera (pyspark backend).

    Enhancements vs the original:
    - Adds aggregated failure summaries:
        * failures by column
        * failures by check (rule name)
    - Keeps a sample of failure cases (sample_failures)
    - Optionally counts total rows (can be expensive on big Spark jobs)
    """

    max_failure_cases: int = 100
    max_groups: int = 50
    count_total_rows: bool = False  # set True if you want report.total for Spark DFs

    def validate_batch(self, plan: ValidationPlan, batch: Any) -> ValidationReport:
        if not isinstance(plan, PanderaPlan):
            raise TypeError(
                f"PanderaBatchValidator expects PanderaPlan, got {type(plan).__name__}"
            )

        def _is_spark_dataframe(obj: object) -> bool:
            # Supports both classic pyspark DF and Spark Connect DF
            return (
                hasattr(obj, "schema")
                and hasattr(obj, "select")
                and hasattr(obj, "limit")
                and hasattr(obj, "collect")
            )

        if not _is_spark_dataframe(batch):
            raise TypeError(
                f"PanderaBatchValidator expects a Spark DataFrame-like object, got {type(batch).__name__}"
            )

        schema_name = getattr(plan, "name", "pandera")

        def _safe_group_counts(df: Any, col: str) -> list[dict[str, Any]]:
            """
            Return [{"value": <col_value>, "count": n}, ...] for df grouped by `col`.
            Works for Spark DF-like objects.
            """
            if not hasattr(df, "groupBy") or not hasattr(df, "count"):
                return []

            cols = getattr(df, "columns", None)
            if not cols or col not in cols:
                return []

            # groupBy(col).count().orderBy(desc("count")).limit(...)
            gb = df.groupBy(col).count()
            if hasattr(gb, "orderBy"):
                try:
                    from pyspark.sql import functions as F
                except ImportError:
                    F = None  # type: ignore[assignment]

                if F is not None:
                    gb = gb.orderBy(F.desc("count"))

            if hasattr(gb, "limit"):
                gb = gb.limit(self.max_groups)

            rows = gb.collect()
            out: list[dict[str, Any]] = []
            for r in rows:
                d = r.asDict(recursive=True) if hasattr(r, "asDict") else dict(r)
                out.append({"value": d.get(col), "count": d.get("count")})
            return out

        def _safe_total(df: Any) -> int | None:
            if not self.count_total_rows:
                return None
            if hasattr(df, "count"):
                try:
                    return int(df.count())
                except Exception:
                    return None
            return None

        try:
            # lazy=True => collect all failures and raise SchemaErrors if any
            plan.schema.validate(batch, lazy=True)

            return ValidationReport(
                schema_name=schema_name,
                total=_safe_total(batch),
                valid=None,
                invalid=0,
                row_errors=[],
                sample_failures=[],
                details={
                    "validator": "pandera",
                    "status": "PASS",
                },
            )

        except SchemaErrors as err:
            failure_df = err.failure_cases

            # Total number of failure rows (not total invalid data rows)
            try:
                failure_count = int(failure_df.count())
            except Exception:
                failure_count = None

            # Sample rows (driver-side)
            try:
                sample_rows = failure_df.limit(self.max_failure_cases).collect()
                sample = [r.asDict(recursive=True) for r in sample_rows]
            except Exception:
                sample = []

            # Summaries (best-effort; depends on what columns failure_cases contains)
            failure_cols = list(getattr(failure_df, "columns", []) or [])

            # Pandera failure_cases usually includes some of these:
            # "column", "check", "check_number", "schema_context", "failure_case", "index"
            by_column = _safe_group_counts(failure_df, "column")
            by_check = _safe_group_counts(failure_df, "check")
            by_schema_context = _safe_group_counts(failure_df, "schema_context")

            details = {
                "validator": "pandera",
                "status": "FAIL",
                "failure_cases_columns": failure_cols,
                "failure_count": failure_count,
                "by_column": by_column,  # [{"value": "age", "count": 10}, ...]
                "by_check": by_check,  # [{"value": "greater_than_or_equal_to(0)", "count": 7}, ...]
                "by_schema_context": by_schema_context,  # optional, varies by backend
            }

            return ValidationReport(
                schema_name=schema_name,
                total=_safe_total(batch),
                valid=None,
                invalid=failure_count,
                row_errors=[],
                sample_failures=sample,
                details=details,
            )
