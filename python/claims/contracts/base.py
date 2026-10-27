"""Base class for generated contract models.

Tolerant-reader rules shared with the Java side (see java/.../common/ContractJson.java):
  * unknown fields are ignored (additive evolution: an old worker can read a newer payload)
  * an unknown enum value becomes UNKNOWN instead of failing the workflow task
"""
from __future__ import annotations

import enum
import typing

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator


def _enum_type(annotation: typing.Any) -> type[enum.Enum] | None:
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return annotation
    for arg in typing.get_args(annotation):
        found = _enum_type(arg)
        if found is not None:
            return found
    return None


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, use_enum_values=False)

    @field_validator("*", mode="wrap")
    @classmethod
    def _unknown_enum_fallback(cls, value, handler, info):
        try:
            return handler(value)
        except ValidationError:
            enum_type = _enum_type(cls.model_fields[info.field_name].annotation)
            if isinstance(value, str) and enum_type is not None and "UNKNOWN" in enum_type.__members__:
                return enum_type["UNKNOWN"]
            raise
