# plans.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol, Type, cast
from pydantic import BaseModel, Field, create_model
from datachecker.schema_loader import SchemaSpec
from pandera import Check
from pandera.pyspark import Column, DataFrameSchema
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType


# -----------------------------
# Common protocol / base types
# -----------------------------


class ValidationPlan(Protocol):
    """Tool-agnostic plan marker (pydantic/pandera etc.)."""

    ...


# ============================================================
# Pydantic plan + compiler (record-level validation in Python)
# ============================================================


@dataclass(frozen=True)
class PydanticPlan(ValidationPlan):
    model: Type[BaseModel]  # compiled model class
    name: str  # schema name (e.g., "user")
    raw_spec: dict[str, Any]  # optional: keep for debugging


_PYDANTIC_TYPE_MAP: dict[str, Any] = {
    "string": str,
    "integer": int,
    "float": float,
    "boolean": bool,
}


def _pydantic_field_kwargs_from_yaml(field_def: dict[str, Any]) -> dict[str, Any]:
    """
    Translate a single YAML field definition into Pydantic Field constraints.
    Supports a small subset: description, str_length.
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

    # If you later add regex support to YAML:
    # - pydantic v2 Field uses `pattern=...`
    # if "regex" in checks: kwargs["pattern"] = checks["regex"]

    return kwargs


class PydanticPlanCompiler:
    """
    Compiles SchemaSpec into a Pydantic model.

    Expected YAML shape (inside spec.raw):
      schema:
        fieldA:
          type: string
          nullable: false
          checks: ...
    """

    def compile(self, spec: SchemaSpec) -> PydanticPlan:
        schema = spec.raw.get("schema")
        if not isinstance(schema, dict):
            raise ValueError(f"{spec.name}: missing or invalid 'schema' block")

        fields: dict[str, tuple[Any, Any]] = {}

        for fname, fdef in schema.items():
            if not isinstance(fdef, dict):
                raise ValueError(f"{spec.name}.{fname}: field def must be dict")

            tname = fdef.get("type")
            if not isinstance(tname, str):
                raise ValueError(f"{spec.name}.{fname}: 'type' must be a string")

            py_type = _PYDANTIC_TYPE_MAP.get(tname)
            if py_type is None:
                raise ValueError(f"{spec.name}.{fname}: unsupported type '{tname}'")

            # nullable default: True
            nullable = bool(fdef.get("nullable", True))
            annotated_type = py_type | None if nullable else py_type

            # required vs optional
            default = None if nullable else ...  # ... means required in pydantic
            field_kwargs = _pydantic_field_kwargs_from_yaml(fdef)
            fields[fname] = (annotated_type, Field(default, **field_kwargs))

        Model = create_model(f"{spec.name.title()}Model", **fields)  # type: ignore[call-overload]
        return PydanticPlan(model=Model, name=spec.name, raw_spec=spec.raw)


# ============================================================
# Pandera plan + compiler (DataFrame-level validation in Spark)
# ============================================================

# plans.py (Pandera section)

_PANDERA_TYPE_MAP = {
    "integer": IntegerType(),
    "string": StringType(),
    "float": DoubleType(),
    "boolean": BooleanType(),
}


@dataclass(frozen=True)
class PanderaPlan:
    schema: DataFrameSchema
    name: str
    raw_spec: dict[str, Any] | None = None


class PanderaPlanCompiler:
    def __init__(self, coerce: bool = False, strict: bool = False):
        self.coerce = coerce
        self.strict = strict

    def compile(self, spec: SchemaSpec) -> PanderaPlan:
        schema_block = spec.raw.get("schema")
        if not isinstance(schema_block, dict):
            raise ValueError(f"{spec.name}: missing or invalid 'schema' block")

        rules_block = spec.raw.get("rules") or {}
        if not isinstance(rules_block, dict):
            raise ValueError(f"{spec.name}: invalid 'rules' block (must be a dict)")

        columns: Dict[str, Column] = {}

        for col_name, col_spec in schema_block.items():
            if not isinstance(col_spec, dict):
                raise ValueError(f"{spec.name}.{col_name}: column def must be dict")

            tname = col_spec.get("type")
            if not isinstance(tname, str):
                raise ValueError(f"{spec.name}.{col_name}: 'type' must be a string")

            spark_type = _PANDERA_TYPE_MAP.get(tname)
            if spark_type is None:
                raise ValueError(f"{spec.name}.{col_name}: unsupported type '{tname}'")

            # ----- 1) nullable from schema (primary source)
            nullable = bool(col_spec.get("nullable", True))

            # ----- 2) rules for this column (from top-level rules block)
            col_rules = rules_block.get(col_name) or {}
            if not isinstance(col_rules, dict):
                raise ValueError(f"{spec.name}.rules.{col_name}: must be a dict")

            # if rules says not_null: true, force nullable=False
            if bool(col_rules.get("not_null", False)):
                nullable = False

            checks: list[Check] = []

            # ----- range {min,max}
            if "range" in col_rules:
                r = col_rules["range"] or {}
                if not isinstance(r, dict):
                    raise ValueError(
                        f"{spec.name}.rules.{col_name}.range must be a dict"
                    )
                if "min" in r:
                    checks.append(Check.ge(r["min"]))
                if "max" in r:
                    checks.append(Check.le(r["max"]))

            # ----- str_length {min,max}
            # NOTE: This assumes pandera supports Check.str_length for pyspark backend.
            # If your pandera version uses a different name, adjust here.
            if "str_length" in col_rules:
                sl = col_rules["str_length"] or {}
                if not isinstance(sl, dict):
                    raise ValueError(
                        f"{spec.name}.rules.{col_name}.str_length must be a dict"
                    )
                min_len = sl.get("min")
                max_len = sl.get("max")
                # only apply if at least one bound is present
                if min_len is not None or max_len is not None:
                    # pandera often uses min_value/max_value naming:
                    # checks.append(Check.str_length(min_value=min_len, max_value=max_len))
                    checks.append(
                        Check.str_length(min_value=min_len, max_value=max_len)
                    )

            # ----- pattern.regex
            if "pattern" in col_rules:
                p = col_rules["pattern"] or {}
                if not isinstance(p, dict):
                    raise ValueError(
                        f"{spec.name}.rules.{col_name}.pattern must be a dict"
                    )
                regex = p.get("regex")
                if regex:
                    # pandera often uses str_matches for regex checks
                    checks.append(Check.str_matches(regex))

            columns[col_name] = Column(
                cast(Any, spark_type),
                nullable=nullable,
                checks=checks,
            )

        schema = DataFrameSchema(
            columns,
            coerce=self.coerce,
            strict=self.strict,
        )

        return PanderaPlan(schema=schema, name=spec.name, raw_spec=spec.raw)
