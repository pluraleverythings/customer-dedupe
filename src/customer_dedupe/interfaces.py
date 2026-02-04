from __future__ import annotations

from typing import Protocol, Sequence

from customer_dedupe.models import Cluster, CustomerRecord, MatchCandidate


class ColumnCleaner(Protocol):
    """Step 1: normalize fields into a canonical representation."""

    def clean(self, records: Sequence[CustomerRecord]) -> list[CustomerRecord]:
        ...


class DeterministicMatcher(Protocol):
    """Step 2: produce exact/rule-based duplicate candidates."""

    def match(self, records: Sequence[CustomerRecord]) -> list[MatchCandidate]:
        ...


class EmbeddingModel(Protocol):
    """Step 3a: map customer records into embedding vectors."""

    def embed(self, records: Sequence[CustomerRecord]) -> list[list[float]]:
        ...


class VectorIndex(Protocol):
    """Step 3b: similarity search over embeddings."""

    def build(self, record_ids: Sequence[str], vectors: Sequence[Sequence[float]]) -> None:
        ...

    def query_similar_pairs(self, min_similarity: float) -> list[MatchCandidate]:
        ...


class EmbeddingMatcher(Protocol):
    """Step 3: end-to-end embedding-based candidate generation + clustering."""

    def match(self, records: Sequence[CustomerRecord]) -> list[MatchCandidate]:
        ...

    def cluster(self, candidates: Sequence[MatchCandidate]) -> list[Cluster]:
        ...


class DedupePipeline(Protocol):
    """Unified pipeline interface for local or distributed execution engines."""

    def run(self, records: Sequence[CustomerRecord]) -> list[Cluster]:
        ...
