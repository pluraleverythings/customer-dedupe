from __future__ import annotations

import random
from collections.abc import Sequence

from customer_dedupe.models import CustomerRecord
from customer_dedupe.schema import FieldTag, RecordSchema

_FIRST_NAMES = [
    "Dominique",
    "Luke",
    "Alex",
    "Sofia",
    "Maya",
    "Daniel",
    "Emma",
    "Chris",
    "Olivia",
    "Noah",
]
_LAST_NAMES = [
    "Smith",
    "Johnson",
    "Brown",
    "Taylor",
    "Wilson",
    "Davies",
    "Martin",
    "Thomas",
]
_STREETS = [
    "Luke Street",
    "Maple Road",
    "King Avenue",
    "River Lane",
    "Elm Street",
    "Station Road",
]
_TOWNS = ["London", "Manchester", "Leeds", "Bristol", "Birmingham", "Dublin"]
_DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "example.com"]


class ReferenceDatasetGenerator:
    """Generate synthetic records (with intentional dupes) for tests and benchmarks."""

    def __init__(self, seed: int = 7) -> None:
        self._rng = random.Random(seed)

    def generate(
        self,
        columns: Sequence[str],
        size: int,
        duplicate_rate: float = 0.15,
        schema: RecordSchema | None = None,
    ) -> list[CustomerRecord]:
        if size <= 0:
            return []

        records: list[CustomerRecord] = []
        unique_count = int(size * (1.0 - duplicate_rate))
        unique_count = max(1, min(unique_count, size))

        column_to_tag = _column_to_tag_map(schema)

        for i in range(unique_count):
            profile = self._profile(i)
            attrs = {
                column: self._value_for_column(column, i, column_to_tag.get(column), profile)
                for column in columns
            }
            records.append(CustomerRecord(record_id=f"cust_{i:07d}", attributes=attrs))

        while len(records) < size:
            source = self._rng.choice(records[:unique_count])
            attrs = dict(source.attributes)
            self._perturb(attrs, column_to_tag)
            records.append(CustomerRecord(record_id=f"cust_{len(records):07d}", attributes=attrs))

        self._rng.shuffle(records)
        return records

    def _profile(self, idx: int) -> dict[str, str]:
        first_name = self._rng.choice(_FIRST_NAMES)
        last_name = self._rng.choice(_LAST_NAMES)
        street = self._rng.choice(_STREETS)
        house_no = str(1 + (idx % 180))
        town = self._rng.choice(_TOWNS)
        country = self._rng.choice(["GB", "US", "IE"])
        email_local = f"{first_name}.{last_name}{idx % 97}".lower()

        return {
            "title": self._rng.choice(["Mr", "Ms", "Dr"]),
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "street": street,
            "house_no": house_no,
            "address_line1": f"{street}, {house_no}",
            "address_line2": "",
            "address_line3": "",
            "town": town,
            "postcode": f"{10000 + (idx % 89999)}",
            "email": f"{email_local}@{self._rng.choice(_DOMAINS)}",
            "phone": f"07{idx % 1000000000:09d}",
            "dob": f"{1970 + (idx % 30)}-{(idx % 12) + 1:02d}-{(idx % 27) + 1:02d}",
            "country": country,
            "date": f"2024-{(idx % 12) + 1:02d}-{(idx % 27) + 1:02d}",
            "marketing": self._rng.choice(["true", "false"]),
        }

    def _value_for_column(
        self,
        column: str,
        idx: int,
        tag: FieldTag | None,
        profile: dict[str, str],
    ) -> str:
        lowered = column.lower()

        if tag == FieldTag.NAME:
            if "title" in lowered:
                return profile["title"]
            if "first" in lowered:
                return profile["first_name"]
            if "last" in lowered:
                return profile["last_name"]
            return profile["full_name"]

        if tag == FieldTag.ADDRESS:
            if "line1" in lowered:
                return profile["address_line1"]
            if "line2" in lowered:
                return profile["address_line2"]
            if "line3" in lowered:
                return profile["address_line3"]
            if "town" in lowered or "city" in lowered:
                return profile["town"]
            return profile["address_line1"]

        if tag == FieldTag.POSTCODE:
            return profile["postcode"]
        if tag == FieldTag.EMAIL:
            return profile["email"]
        if tag == FieldTag.PHONE:
            return profile["phone"]
        if tag == FieldTag.DOB:
            return profile["dob"]
        if tag == FieldTag.COUNTRY:
            return profile["country"]
        if tag == FieldTag.DATE:
            return profile["date"]
        if tag == FieldTag.MARKETING:
            return profile["marketing"]

        if "count" in lowered:
            return str(idx % 4)
        if "email" in lowered:
            return profile["email"]
        if "postcode" in lowered or "zip" in lowered:
            return profile["postcode"]
        if "address" in lowered:
            return profile["address_line1"]
        if "town" in lowered or "city" in lowered:
            return profile["town"]
        if "first" in lowered:
            return profile["first_name"]
        if "last" in lowered:
            return profile["last_name"]
        if "name" in lowered:
            return profile["full_name"]
        return f"{column}_{idx:07d}"

    def _perturb(self, attrs: dict[str, str], column_to_tag: dict[str, FieldTag]) -> None:
        email_cols = self._columns_for(attrs, column_to_tag, FieldTag.EMAIL, contains=("email",))
        first_name_cols = [c for c in attrs if "first" in c.lower()]
        last_name_cols = [c for c in attrs if "last" in c.lower()]
        name_cols = self._columns_for(attrs, column_to_tag, FieldTag.NAME, contains=("name",))
        address_cols = self._columns_for(attrs, column_to_tag, FieldTag.ADDRESS, contains=("address",))

        mutation = self._rng.choice(["email", "name", "address", "mixed"])

        if mutation in {"email", "mixed"} and email_cols:
            col = self._rng.choice(email_cols)
            attrs[col] = self._email_variant(attrs[col])

        if mutation in {"name", "mixed"} and name_cols:
            self._name_variant(attrs, first_name_cols, last_name_cols, name_cols)

        if mutation in {"address", "mixed"} and address_cols:
            self._address_variant(attrs, address_cols)

    def _columns_for(
        self,
        attrs: dict[str, str],
        column_to_tag: dict[str, FieldTag],
        tag: FieldTag,
        contains: tuple[str, ...],
    ) -> list[str]:
        cols = [column for column in attrs if column_to_tag.get(column) == tag]
        if cols:
            return cols
        return [column for column in attrs if any(token in column.lower() for token in contains)]

    def _email_variant(self, email: str) -> str:
        if "@" not in email:
            return email
        local, domain = email.split("@", maxsplit=1)
        variant = self._rng.choice(["plus", "dot", "case"])

        if variant == "plus":
            suffix = self._rng.choice(["test", "shop", "vip"])
            return f"{local}+{suffix}@{domain}"
        if variant == "dot" and len(local) > 3 and "." not in local:
            insert_at = max(1, len(local) // 2)
            return f"{local[:insert_at]}.{local[insert_at:]}@{domain}"
        return f"{local.capitalize()}@{domain}"

    def _name_variant(
        self,
        attrs: dict[str, str],
        first_name_cols: list[str],
        last_name_cols: list[str],
        name_cols: list[str],
    ) -> None:
        if first_name_cols:
            first_col = self._rng.choice(first_name_cols)
            first_value = attrs[first_col].strip()
            lowered = first_value.lower()
            if lowered.startswith("dom"):
                attrs[first_col] = self._rng.choice(["Dom", "Dominique"])
            elif len(first_value) > 4:
                attrs[first_col] = first_value[:3]
            else:
                expansion = {"alex": "Alexander", "chris": "Christopher", "dan": "Daniel"}
                attrs[first_col] = expansion.get(lowered, first_value)

        if last_name_cols:
            last_col = self._rng.choice(last_name_cols)
            last = attrs[last_col].strip()
            attrs[last_col] = self._rng.choice([last.upper(), last.lower(), last[:-1] if len(last) > 4 else last])

        for col in name_cols:
            if "first" in col.lower() or "last" in col.lower() or "title" in col.lower():
                continue
            parts = []
            if first_name_cols:
                parts.append(attrs[first_name_cols[0]])
            if last_name_cols:
                parts.append(attrs[last_name_cols[0]])
            if parts:
                attrs[col] = " ".join(parts)

    def _address_variant(self, attrs: dict[str, str], address_cols: list[str]) -> None:
        line1_cols = [c for c in address_cols if "line1" in c.lower()]
        line2_cols = [c for c in address_cols if "line2" in c.lower()]

        target_cols = line1_cols or address_cols
        col = self._rng.choice(target_cols)
        value = attrs[col].strip()

        if "street" in value.lower():
            variant = value.replace("Street", "St").replace("street", "st")
        elif " st" in value.lower() or value.lower().endswith("st"):
            variant = value.replace(" St", " Street").replace(" st", " street")
        else:
            variant = value

        if self._rng.random() < 0.6:
            variant = f"{variant}, Top floor"

        attrs[col] = variant

        if line2_cols and self._rng.random() < 0.5:
            attrs[self._rng.choice(line2_cols)] = self._rng.choice(["Top floor", "Flat 2", "Apt 5"])


def _column_to_tag_map(schema: RecordSchema | None) -> dict[str, FieldTag]:
    if schema is None:
        return {}
    mapping: dict[str, FieldTag] = {}
    for tag, columns in schema.tag_to_columns.items():
        for column in columns:
            mapping[column] = tag
    return mapping
