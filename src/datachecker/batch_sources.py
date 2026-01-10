from __future__ import annotations

import csv
import json

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, runtime_checkable


# ---- Common types ----

Record = Mapping[str, Any]

# ---- Interface ----


@runtime_checkable
class BatchSource(Protocol):
    """Reads data from files and yields records or batches."""

    def read(self) -> Iterator[Record]: ...

    # optionally:
    # def read_batches(self) -> Iterator[list[Record]]: ...


# ---- CSV plugin ----


@dataclass(frozen=True)
class CsvBatchSource(BatchSource):
    path: str | Path
    delimiter: str = ","
    encoding: str = "utf-8"
    newline: str = ""

    def read(self) -> Iterator[Record]:
        """
        Yields one record (row) at a time as a dict: {column_name: value}.
        Values are strings by default (that's how csv works).
        """
        path = Path(self.path)
        with path.open(mode="r", encoding=self.encoding, newline=self.newline) as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                # row is a dict[str, str | None]; we return it as Mapping[str, Any]
                yield row

    def read_batches(self, batch_size: int = 1000) -> Iterator[list[Record]]:
        """
        Optional: yields lists of records (batches).
        Useful if downstream validators/checkers work in chunks.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        batch: list[Record] = []
        for rec in self.read():
            batch.append(rec)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


@dataclass(frozen=True)
class JsonlBatchSource(BatchSource):
    """
    Reads JSON Lines (JSONL): one JSON object per line.

    Compatible with input like:
    {"id": 1, "first_name": "...", ...}
    {"id": 2, "first_name": "...", ...}
    """

    path: str | Path
    encoding: str = "utf-8"

    def read(self) -> Iterator[Record]:
        path = Path(self.path)
        with path.open("r", encoding=self.encoding) as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Invalid JSON on line {line_no} in {path}: {e}"
                    ) from e

                if not isinstance(obj, dict):
                    raise ValueError(
                        f"Expected a JSON object (dict) on line {line_no} in {path}, "
                        f"got {type(obj).__name__}"
                    )

                yield obj

    def read_batches(self, batch_size: int = 1000) -> Iterator[list[Record]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        batch: list[Record] = []
        for rec in self.read():
            batch.append(rec)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
