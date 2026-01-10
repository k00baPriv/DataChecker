from __future__ import annotations
from dataclasses import dataclass
from datachecker.schema_loader import SchemaSpec
from typing import Any, Type
from pydantic import BaseModel, Field, create_model


@dataclass(frozen=True)
class PydanticPlan:
    model: Type[BaseModel]  # compiled model class
    name: str  # schema name (e.g., "user")
    raw_spec: dict[str, Any]  # optional: keep for debugging


TYPE_MAP: dict[str, Any] = {
    "string": str,
    "integer": int,
    "float": float,
    "boolean": bool,
}


def _field_kwargs_from_yaml(field_def: dict[str, Any]) -> dict[str, Any]:
    """
    Translate a single YAML field definition into Pydantic Field constraints.
    Supports a small subset: nullable, str_length, regex.
    """
    checks = field_def.get("checks") or {}
    desc = field_def.get("description")

    kwargs: dict[str, Any] = {}
    if desc:
        kwargs["description"] = desc

    # string length
    if "str_length" in checks:
        sl = checks["str_length"] or {}
        # YAML uses min_value/max_value; Pydantic uses min_length/max_length
        if "min_value" in sl:
            kwargs["min_length"] = sl["min_value"]
        if "max_value" in sl:
            kwargs["max_length"] = sl["max_value"]

    # regex (if you decide to also read it from rules later)
    # kwargs["pattern"] = ...  # Pydantic v2 uses 'pattern' for Field
    return kwargs


class PydanticPlanCompiler:
    def compile(self, spec: SchemaSpec) -> PydanticPlan:
        schema = spec.raw.get("schema")
        if not isinstance(schema, dict):
            raise ValueError(f"{spec.name}: missing schema block")

        fields: dict[str, tuple[Any, Any]] = {}

        for fname, fdef in schema.items():
            if not isinstance(fdef, dict):
                raise ValueError(f"{spec.name}.{fname}: field def must be dict")

            tname = fdef.get("type")
            if not isinstance(tname, str):
                raise ValueError(f"{spec.name}.{fname}: 'type' must be a string")
            py_type = TYPE_MAP.get(tname)
            if py_type is None:
                raise ValueError(f"{spec.name}.{fname}: unsupported type '{tname}'")

            nullable = bool(fdef.get("nullable", True))
            annotated_type = py_type | None if nullable else py_type

            # required vs optional
            default = None if nullable else ...  # ... means required
            field_kwargs = _field_kwargs_from_yaml(fdef)
            fields[fname] = (annotated_type, Field(default, **field_kwargs))

        Model = create_model(f"{spec.name.title()}Model", **fields)  # type: ignore[call-overload]

        return PydanticPlan(model=Model, name=spec.name, raw_spec=spec.raw)
