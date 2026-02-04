from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, Sequence


class FieldTag(StrEnum):
    ADDRESS = "ADDRESS"
    COUNTRY = "COUNTRY"
    CUSTOMER_ID = "CUSTOMER_ID"
    DATE = "DATE"
    DOB = "DOB"
    EMAIL = "EMAIL"
    GENDER = "GENDER"
    MARKETING = "MARKETING"
    NAME = "NAME"
    PHONE = "PHONE"
    POSTCODE = "POSTCODE"


@dataclass(frozen=True)
class RecordSchema:
    """Maps source-system columns to stable semantic tags."""

    tag_to_columns: Mapping[FieldTag, tuple[str, ...]]

    @classmethod
    def from_mapping(cls, mapping: Mapping[FieldTag, Sequence[str]]) -> "RecordSchema":
        frozen = {tag: tuple(columns) for tag, columns in mapping.items()}
        return cls(tag_to_columns=frozen)

    def columns_for(self, tag: FieldTag) -> tuple[str, ...]:
        return self.tag_to_columns.get(tag, ())

    def values_for(self, attributes: Mapping[str, object], tag: FieldTag) -> list[str]:
        values: list[str] = []
        for column in self.columns_for(tag):
            value = attributes.get(column)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                values.append(text)
        return values

    def joined_value(self, attributes: Mapping[str, object], tag: FieldTag, sep: str = " ") -> str:
        return sep.join(self.values_for(attributes, tag)).strip()
