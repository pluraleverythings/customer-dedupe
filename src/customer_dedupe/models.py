from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class CustomerRecord:
    """Canonical representation of a customer account record."""

    record_id: str
    attributes: dict[str, Any]


@dataclass(slots=True)
class MatchCandidate:
    """Potential duplicate pair with an attached confidence score."""

    left_id: str
    right_id: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Cluster:
    """A collection of record ids that likely refer to the same entity."""

    cluster_id: str
    record_ids: list[str]
    confidence: float
