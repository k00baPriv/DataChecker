from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Protocol

# If you already define Record elsewhere, import it instead.
Record = Mapping[str, Any]


# -----------------------------
# Protocols (engine contracts)
# -----------------------------


class SchemaSpec(Protocol):
    name: str


class SchemaLoader(Protocol):
    def load(self, schema_ref: str) -> SchemaSpec: ...


class ValidationPlan(Protocol): ...


class PlanCompiler(Protocol):
    def compile(self, spec: SchemaSpec) -> ValidationPlan: ...


class BatchSource(Protocol):
    """Reads data from files and yields records or batches."""

    def read(self) -> Iterator[Record]: ...

    # optional but recommended for batch mode / streaming:
    def read_batches(self, batch_size: int = 1000) -> Iterator[list[Record]]: ...


class ReportSink(Protocol):
    def write(self, payload: Mapping[str, Any]) -> None: ...


class RecordValidator(Protocol):
    def validate_record(
        self, plan: ValidationPlan, record: Record
    ) -> tuple[bool, Any | None]: ...


class BatchValidator(Protocol):
    def validate_batch(self, plan: ValidationPlan, records: list[Record]) -> Any: ...


# -----------------------------
# Engine
# -----------------------------


@dataclass(frozen=True)
class ValidationEngine:
    """
    Orchestrates: schema loading -> plan compilation -> reading data -> validating -> writing reports.

    Configure with EXACTLY ONE of:
      - record_validator (record-by-record)
      - batch_validator  (batch-by-batch)

    Notes:
    - record mode streams and can emit incremental chunks to the sink.
    - batch mode uses source.read_batches() to avoid loading all data into memory.
    """

    schema_loader: SchemaLoader
    compiler: PlanCompiler
    source: BatchSource
    sink: ReportSink

    record_validator: RecordValidator | None = None
    batch_validator: BatchValidator | None = None

    batch_size: int = 1000
    emit: str = "chunk"  # "chunk" (incremental) or "final" (single payload at end)

    def __post_init__(self) -> None:
        # exactly one must be provided
        if (self.record_validator is None) == (self.batch_validator is None):
            raise ValueError(
                "Provide exactly one of: record_validator OR batch_validator."
            )

    def run(self, schema_ref: str) -> None:
        spec = self.schema_loader.load(schema_ref)
        plan = self.compiler.compile(spec)

        schema_name = getattr(spec, "name", schema_ref)

        self.sink.write({"event": "start", "schema": schema_name})

        if self.record_validator is not None:
            self._run_record_mode(schema_name=schema_name, plan=plan)
        else:
            self._run_batch_mode(schema_name=schema_name, plan=plan)

        self.sink.write({"event": "end", "schema": schema_name})

    # -----------------------------
    # Record mode
    # -----------------------------
    def _run_record_mode(self, schema_name: str, plan: ValidationPlan) -> None:
        total = ok = bad = 0
        all_results: list[dict[str, Any]] = []

        # Prefer read_batches if available; else fallback to read()
        read_batches = getattr(self.source, "read_batches", None)

        if callable(read_batches):
            batches = read_batches(batch_size=self.batch_size)
            for batch_no, batch in enumerate(batches, start=1):
                chunk: list[dict[str, Any]] = []

                for i, rec in enumerate(batch):
                    total += 1
                    is_ok, err = self.record_validator.validate_record(plan, rec)  # type: ignore[union-attr]
                    if is_ok:
                        ok += 1
                    else:
                        bad += 1

                    chunk.append(
                        {
                            "row_index": (batch_no - 1) * self.batch_size + i,
                            "ok": is_ok,
                            "error": err,
                        }
                    )

                if self.emit == "chunk":
                    self.sink.write(
                        {
                            "event": "chunk",
                            "mode": "record",
                            "schema": schema_name,
                            "batch_no": batch_no,
                            "stats": {"total": total, "ok": ok, "bad": bad},
                            "results": chunk,
                        }
                    )
                else:
                    all_results.extend(chunk)

        else:
            # fallback: stream per record
            for idx, rec in enumerate(self.source.read()):
                total += 1
                is_ok, err = self.record_validator.validate_record(plan, rec)  # type: ignore[union-attr]
                if is_ok:
                    ok += 1
                else:
                    bad += 1

                row = {"row_index": idx, "ok": is_ok, "error": err}

                if self.emit == "chunk":
                    self.sink.write(
                        {
                            "event": "row",
                            "mode": "record",
                            "schema": schema_name,
                            "stats": {"total": total, "ok": ok, "bad": bad},
                            "result": row,
                        }
                    )
                else:
                    all_results.append(row)

        if self.emit == "final":
            self.sink.write(
                {
                    "event": "final",
                    "mode": "record",
                    "schema": schema_name,
                    "stats": {"total": total, "ok": ok, "bad": bad},
                    "results": all_results,
                }
            )
        else:
            # still useful to emit a final summary even when chunking
            self.sink.write(
                {
                    "event": "summary",
                    "mode": "record",
                    "schema": schema_name,
                    "stats": {"total": total, "ok": ok, "bad": bad},
                }
            )

    # -----------------------------
    # Batch mode
    # -----------------------------
    def _run_batch_mode(self, schema_name: str, plan: ValidationPlan) -> None:
        if not callable(getattr(self.source, "read_batches", None)):
            raise ValueError(
                "Batch mode requires source.read_batches(batch_size=...). "
                "Your BatchSource should implement read_batches()."
            )

        total_batches = 0
        for batch_no, batch in enumerate(
            self.source.read_batches(batch_size=self.batch_size), start=1
        ):
            total_batches += 1
            report = self.batch_validator.validate_batch(plan, batch)  # type: ignore[union-attr]

            # Stream each batch report (recommended)
            self.sink.write(
                {
                    "event": "chunk",
                    "mode": "batch",
                    "schema": schema_name,
                    "batch_no": batch_no,
                    "batch_size": len(batch),
                    "report": report,
                }
            )

        self.sink.write(
            {
                "event": "summary",
                "mode": "batch",
                "schema": schema_name,
                "total_batches": total_batches,
            }
        )
