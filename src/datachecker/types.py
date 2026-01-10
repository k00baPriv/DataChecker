from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence, TypeAlias, Literal


# -----------------
# Core aliases
# -----------------

Record: TypeAlias = Mapping[str, Any]


# -----------------
# Errors
# -----------------


@dataclass(frozen=True)
class FieldError:
    field: str
    message: str
    code: str | None = None


@dataclass(frozen=True)
class RowError:
    row_index: int
    errors: Sequence[FieldError]
    record_hint: Mapping[str, Any] | None = None


# -----------------
# Reports
# -----------------


@dataclass(frozen=True)
class ValidationStats:
    total: int
    ok: int
    bad: int


@dataclass(frozen=True)
class ValidationReport:
    schema: str
    stats: ValidationStats
    row_errors: Sequence[RowError]

    def ok(self) -> bool:
        return self.stats.bad == 0


# -----------------
# Engine events (optional)
# -----------------

EventType = Literal["start", "chunk", "row", "summary", "end"]


@dataclass(frozen=True)
class EngineEvent:
    event: EventType
    schema: str
    payload: Mapping[str, Any]
