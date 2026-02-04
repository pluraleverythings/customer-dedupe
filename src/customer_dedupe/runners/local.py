from __future__ import annotations

from collections.abc import Sequence

from customer_dedupe.interfaces import ColumnCleaner, DeterministicMatcher
from customer_dedupe.models import Cluster, CustomerRecord
from customer_dedupe.steps.embedding import DefaultEmbeddingMatcher


class LocalDedupePipeline:
    """Local runner suitable for development on large single-machine datasets."""

    def __init__(
        self,
        cleaner: ColumnCleaner,
        deterministic_matcher: DeterministicMatcher,
        embedding_matcher: DefaultEmbeddingMatcher,
    ) -> None:
        self._cleaner = cleaner
        self._deterministic_matcher = deterministic_matcher
        self._embedding_matcher = embedding_matcher

    def run(self, records: Sequence[CustomerRecord]) -> list[Cluster]:
        cleaned = self._cleaner.clean(records)
        deterministic_candidates = self._deterministic_matcher.match(cleaned)
        embedding_candidates = self._embedding_matcher.match(cleaned)

        all_candidates = deterministic_candidates + embedding_candidates
        return self._embedding_matcher.cluster(all_candidates)
