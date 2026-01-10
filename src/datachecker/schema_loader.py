from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import yaml
# --- In-memory schema representation ---


@dataclass(frozen=True)
class SchemaSpec:
    """Neutral, tool-agnostic schema representation produced from YAML."""

    name: str
    version: str | None
    raw: dict[str, Any]


# --- Schema definition layer (YAML -> in-memory schema) ---


@runtime_checkable
class SchemaLoader(Protocol):
    """Loads schema definition from YAML (or other source)."""

    def load(self, schema_ref: str) -> SchemaSpec: ...


class YamlFileSchemaLoader:
    """
    Loads schemas from YAML files.

    Examples:
      loader = YamlFileSchemaLoader(root_dir="schemas")
      spec = loader.load("user")              # -> schemas/user.yaml
      spec = loader.load("schemas/user.yaml") # direct path
    """

    def __init__(self, root_dir: str | Path = "schemas") -> None:
        self.root_dir = Path(root_dir)

    def load(self, schema_ref: str) -> SchemaSpec:
        path = self._resolve(schema_ref)
        data = self._read_yaml(path)

        # Expected structure like:
        # user:
        #   schema: {...}
        #   rules:  {...}
        if not isinstance(data, dict) or len(data) != 1:
            raise ValueError(
                f"Schema YAML must have exactly one top-level key (e.g., 'user'). Got: {list(data) if isinstance(data, dict) else type(data)}"
            )

        name, body = next(iter(data.items()))
        if not isinstance(name, str) or not isinstance(body, dict):
            raise ValueError("Top-level schema key must map to a dict.")

        schema_block = body.get("schema")
        rules_block = body.get("rules")
        version = body.get("version")  # optional

        if not isinstance(schema_block, dict):
            raise ValueError(
                f"Missing or invalid '{name}.schema' block (must be a dict)."
            )
        if rules_block is not None and not isinstance(rules_block, dict):
            raise ValueError(
                f"Invalid '{name}.rules' block (must be a dict if present)."
            )
        if version is not None and not isinstance(version, (str, int, float)):
            raise ValueError(f"Invalid '{name}.version' (must be scalar if present).")

        # Keep the raw structure flexible; downstream layers interpret it
        return SchemaSpec(
            name=name,
            version=str(version) if version is not None else None,
            raw=body,
        )

    def _resolve(self, schema_ref: str) -> Path:
        p = Path(schema_ref)

        # If user passed a path that exists, use it
        if p.exists() and p.is_file():
            return p

        # Otherwise interpret schema_ref as a schema name like "user"
        # and resolve to <root_dir>/<schema_ref>.yaml
        candidate = (self.root_dir / f"{schema_ref}.yaml").resolve()
        if not candidate.exists():
            raise FileNotFoundError(
                f"Schema file not found for ref '{schema_ref}'. Tried: {candidate}"
            )
        return candidate

    def _read_yaml(self, path: Path) -> dict[str, Any]:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as e:
            raise OSError(f"Failed to read schema file: {path}") from e

        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in schema file: {path}") from e

        if data is None:
            raise ValueError(f"Schema file is empty: {path}")
        if not isinstance(data, dict):
            raise ValueError(
                f"Schema YAML must parse to a dict at root. Got: {type(data)}"
            )
        return data
