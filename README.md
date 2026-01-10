Here’s a clean, professional README.md tailored to what you’ve built so far.
You can copy-paste this directly into README.md and adjust wording later.

⸻


# DataChecker.   

**DataChecker** is a small, extensible Python framework for **data quality validation in batch pipelines**.

The goal of the project is to show how you can:
- define **data schemas** in YAML 
- compile them into executable validation plans
- validate **records or batches** from different sources (CSV, JSONL, …)
- keep the system **tool-agnostic** (Pydantic, Pandera, custom rules)
- enforce correctness using **Python typing, Protocols, and mypy**

This repository is intentionally designed as a **learning-friendly but production-ready architecture**.

---

## Key ideas

- **Clear contracts** using `typing.Protocol`
- **Pluggable components**:
  - batch sources (CSV, JSONL, future Parquet, DB, API…)
  - validators (record-level, batch-level)
  - sinks (console, JSONL, future DB / monitoring systems)
- **Schema-first validation** using YAML
- Strong emphasis on:
  - type safety (`mypy`)
  - test coverage (`pytest`)
  - code quality (`ruff`)


## Architecture Overview

The diagram below shows the high-level architecture of the DataChecker engine,
including schema loading, plan compilation, batch processing, validation, and reporting.

```mermaid
classDiagram
direction LR

class ValidationEngine {
  -schema_loader: SchemaLoader
  -plan_compiler: PlanCompiler
  -source: BatchSource
  -validator: BatchValidator
  -sink: Sink
  +run(schema_ref, source_ref, report_ref)
}

class SchemaLoader {
  <<interface>>
  +load(schema_ref) SchemaSpec
}

class YamlSchemaLoader {
  +load(schema_ref) SchemaSpec
}
SchemaLoader <|.. YamlSchemaLoader

class SchemaSpec {
  <<dataclass>>
  +name: str
  +version: str
  +raw: dict
}

class PlanCompiler {
  <<interface>>
  +compile(spec) ValidationPlan
}

class DefaultPlanCompiler {
  +compile(spec) ValidationPlan
}
PlanCompiler <|.. DefaultPlanCompiler

class ValidationPlan {
  <<interface>>
}

class BatchSource {
  <<interface>>
  +open(source_ref)
  +iter_records()
  +close()
}

class CsvBatchSource
class JsonlBatchSource
BatchSource <|.. CsvBatchSource
BatchSource <|.. JsonlBatchSource

class BatchValidator {
  <<interface>>
  +validate(plan, records) ValidationReport
}

class PydanticBatchValidator
BatchValidator <|.. PydanticBatchValidator

class Sink {
  <<interface>>
  +write(report, target_ref)
}

class ConsoleSink
class JsonlSink
Sink <|.. ConsoleSink
Sink <|.. JsonlSink

class ValidationReport {
  +total_records: int
  +valid_records: int
  +invalid_records: int
  +errors: list
}

ValidationEngine --> SchemaLoader
ValidationEngine --> PlanCompiler
ValidationEngine --> BatchSource
ValidationEngine --> BatchValidator
ValidationEngine --> Sink


## Project structure

```text
src/datachecker/
├── batch_sources.py      # CSV / JSONL batch readers
├── batch_validators.py   # Batch-level validators (e.g. Pydantic)
├── engine.py             # ValidationEngine orchestration
├── plans.py              # Schema → validation plan compilation
├── schema_loader.py      # YAML schema loading
├── sinks.py              # Output sinks (print, jsonl, etc.)
├── types.py              # Shared data structures & Protocols
└── __init__.py
```

Supporting directories:
	•	schemas/ – YAML schema definitions
	•	data/ – sample input data
	•	tests/ – unit tests
	•	scripts/ – developer tooling (checks, fixes)
	•	study/ – notebooks used to explore and explain the architecture

⸻

Installation

Requirements
	•	Python 3.10+
	•	pip

Create and activate a virtual environment first (recommended).

⸻

Install runtime dependencies only

For users who just want to use the library:

pip install .


⸻

Install development environment (recommended for contributors)

pip install -e ".[dev]"

This installs:
- pytest, pytest-cov
- mypy 
- ruff 
- pip-audit 
- type stubs (types-PyYAML)
- your package in editable mode

⸻

Install notebook environment (optional)

If you want to run the notebooks in study/:

pip install -e ".[notebooks]"

Or combine:

pip install -e ".[dev,notebooks]"


⸻

Developer workflow

This project follows a clear separation between:
- code fixing 
- code checking

Auto-fix code (local development)

python scripts/fix.py

This will:
- remove unused imports 
- normalize imports 
- apply Ruff formatting

⚠️ This modifies source files.

⸻

Verify everything (tests + types + linting)

python scripts/check.py

This runs:
- Ruff formatting check 
- Rufff lint checks
- mypy type checks
- pytest + coverage
- dependency vulnerability scan (pip-audit)

This script never modifies code and is safe for CI.

⸻

Running tests manually

pytest

Coverage reports:
- terminal output 
- coverage.xml
- htmlcov/index.html

⸻

Schema-driven validation (high level)
- Define a schema in YAML (example: schemas/user.yaml)
- Load schema via SchemaLoader
- Compile schema into a validation plan
- Read data via a BatchSource
- Validate records or batches
- Emit reports via a Sink

This design allows you to swap:
- validation backends 
- input sources 
- output destinations

without touching the engine.
