from __future__ import annotations

from pathlib import Path

import pytest

from datachecker.batch_sources import CsvBatchSource


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_read_yields_dict_rows(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(
        p,
        "id;name\n"
        "1;Alice\n"
        "2;Bob\n",
    )

    src = CsvBatchSource(p, delimiter=";")
    rows = list(src.read())

    assert rows == [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]


def test_read_respects_comma_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(
        p,
        "id,name\n"
        "1,Alice\n",
    )

    src = CsvBatchSource(p, delimiter=",")
    rows = list(src.read())

    assert rows == [{"id": "1", "name": "Alice"}]


def test_read_empty_after_header(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(p, "id;name\n")

    src = CsvBatchSource(p, delimiter=";")
    rows = list(src.read())

    assert rows == []


def test_read_batches_exact_multiple(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(
        p,
        "id;name\n"
        "1;A\n"
        "2;B\n"
        "3;C\n"
        "4;D\n",
    )

    src = CsvBatchSource(p, delimiter=";")
    batches = list(src.read_batches(batch_size=2))

    assert len(batches) == 2
    assert [len(b) for b in batches] == [2, 2]
    assert batches[0][0]["id"] == "1"
    assert batches[1][0]["id"] == "3"


def test_read_batches_with_remainder(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(
        p,
        "id;name\n"
        "1;A\n"
        "2;B\n"
        "3;C\n",
    )

    src = CsvBatchSource(p, delimiter=";")
    batches = list(src.read_batches(batch_size=2))

    assert [len(b) for b in batches] == [2, 1]
    assert batches[0] == [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]
    assert batches[1] == [{"id": "3", "name": "C"}]


def test_read_batches_batch_size_must_be_positive(tmp_path: Path) -> None:
    p = tmp_path / "users.csv"
    write_text(p, "id;name\n1;A\n")

    src = CsvBatchSource(p, delimiter=";")

    with pytest.raises(ValueError, match="batch_size must be > 0"):
        list(src.read_batches(batch_size=0))

    with pytest.raises(ValueError, match="batch_size must be > 0"):
        list(src.read_batches(batch_size=-10))