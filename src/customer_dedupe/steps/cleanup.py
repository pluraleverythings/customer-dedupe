from __future__ import annotations

from collections.abc import Callable, Sequence

from customer_dedupe.models import CustomerRecord
from customer_dedupe.schema import FieldTag, RecordSchema


class FunctionalCleaner:
    """Composable cleaner supporting both raw column and semantic-tag transforms."""

    def __init__(
        self,
        transforms: dict[str, Callable[[object], object]] | None = None,
        tag_transforms: dict[FieldTag, Callable[[str], str]] | None = None,
        schema: RecordSchema | None = None,
    ) -> None:
        self._transforms = transforms or {}
        self._tag_transforms = tag_transforms or {}
        self._schema = schema

    def clean(self, records: Sequence[CustomerRecord]) -> list[CustomerRecord]:
        cleaned: list[CustomerRecord] = []
        for record in records:
            attrs = dict(record.attributes)

            for field, transform in self._transforms.items():
                if field in attrs:
                    attrs[field] = transform(attrs[field])

            if self._schema:
                for tag, transform in self._tag_transforms.items():
                    for column in self._schema.columns_for(tag):
                        value = attrs.get(column)
                        if value is None:
                            continue
                        attrs[column] = transform(str(value))

            cleaned.append(CustomerRecord(record_id=record.record_id, attributes=attrs))
        return cleaned
