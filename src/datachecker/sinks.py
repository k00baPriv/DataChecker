from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol


class ReportSink(Protocol):
    """Where the engine writes outputs (events, reports, metrics, etc.)."""

    def write(self, payload: Mapping[str, Any]) -> None: ...


def _to_jsonable(obj: Any) -> Any:
    """
    Best-effort conversion to JSON-serializable objects.
    - dataclasses -> dict
    - Path -> str
    - everything else -> str fallback (via json.dumps(default=str))
    """

    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, Path):
        return str(obj)
    return obj


# ------------------------
# Simple sinks
# ------------------------


class NullSink:
    """Drops everything (useful in tests)."""

    def write(self, payload: Mapping[str, Any]) -> None:
        return


class PrintSink:
    """Prints each payload as pretty JSON (nice for notebooks)."""

    def __init__(self, pretty: bool = True) -> None:
        self.pretty = pretty

    def write(self, payload: Mapping[str, Any]) -> None:
        if self.pretty:
            print(
                json.dumps(
                    payload,
                    default=lambda o: _to_jsonable(o),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        else:
            print(
                json.dumps(
                    payload, default=lambda o: _to_jsonable(o), ensure_ascii=False
                )
            )


class JsonlSink:
    """
    Writes one JSON object per line (JSONL).
    Best for streaming events and large runs.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Mapping[str, Any]) -> None:
        line = json.dumps(
            payload, default=lambda o: _to_jsonable(o), ensure_ascii=False
        )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


class JsonSink:
    """
    Writes a single JSON document (overwrites each time).
    Use only if you write once at the end (emit="final").
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Mapping[str, Any]) -> None:
        text = json.dumps(
            payload, default=lambda o: _to_jsonable(o), ensure_ascii=False, indent=2
        )
        self.path.write_text(text, encoding="utf-8")
