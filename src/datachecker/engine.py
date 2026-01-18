# datachecker/engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

from datachecker.types import BatchValidator, ValidationPlan, ValidationReport


@dataclass
class ValidationEngine:
    schema_loader: Any  # expects .load(schema_ref) -> SchemaSpec
    compiler: Any  # expects .compile(spec) -> ValidationPlan
    source: Any  # expects .read_batches(batch_size=...) -> Iterator[Any]
    sink: Any  # expects .write(obj) -> None

    batch_validator: Optional[BatchValidator] = None

    # Optional: if you have record-mode in your engine, keep it separate.
    record_validator: Any | None = None

    batch_size: int = 1000

    def run(self, schema_ref: str) -> None:
        spec = self.schema_loader.load(schema_ref)
        plan: ValidationPlan = self.compiler.compile(spec)

        schema_name = getattr(spec, "name", schema_ref)
        self.sink.write({"event": "start", "schema": schema_name})

        if self.batch_validator is not None:
            self._run_batch_mode(schema_name=schema_name, plan=plan)
        elif self.record_validator is not None:
            self._run_record_mode(schema_name=schema_name, plan=plan)
        else:
            raise ValueError(
                "No validator configured: set batch_validator or record_validator"
            )

        self.sink.write({"event": "end", "schema": schema_name})

    def _run_batch_mode(self, schema_name: str, plan: ValidationPlan) -> None:
        if self.batch_validator is None:
            raise RuntimeError("batch_validator is required for batch mode")

        total_invalid = 0
        batch_no = 0

        for batch_no, batch in enumerate(self._iter_batches(), start=1):
            report: ValidationReport = self.batch_validator.validate_batch(plan, batch)
            total_invalid += report.invalid or 0

            self.sink.write(
                {
                    "event": "chunk",
                    "mode": "batch",
                    "schema": schema_name,
                    "batch_no": batch_no,
                    # Don't call len() here; Spark DF has no len().
                    "batch_size": report.total,
                    "report": self._report_to_dict(report),
                }
            )

        self.sink.write(
            {
                "event": "summary",
                "mode": "batch",
                "schema": schema_name,
                "batches": batch_no,
                "invalid_total": total_invalid,
            }
        )

    def _run_record_mode(self, schema_name: str, plan: ValidationPlan) -> None:
        # Kept only if you already have record-mode. If not needed, remove.
        raise NotImplementedError("record_mode not wired in this simplified engine")

    def _iter_batches(self) -> Iterator[Any]:
        # your sources may define read_batches(batch_size=...) or read_batches()
        rb = getattr(self.source, "read_batches", None)
        if rb is None:
            raise TypeError("Source must implement read_batches(...)")

        try:
            yield from rb(batch_size=self.batch_size)
        except TypeError:
            # If source.read_batches() doesn't accept batch_size
            yield from rb()

    def _report_to_dict(self, report: ValidationReport) -> dict[str, Any]:
        return {
            "schema_name": report.schema_name,
            "total": report.total,
            "valid": report.valid,
            "invalid": report.invalid,
            "row_errors": [
                {
                    "row_index": e.row_index,
                    "record_hint": e.record_hint,
                    "errors": e.errors,
                }
                for e in (report.row_errors or [])
            ],
            "sample_failures": report.sample_failures or [],
            "details": report.details or {},
            "ok": report.ok(),
        }
