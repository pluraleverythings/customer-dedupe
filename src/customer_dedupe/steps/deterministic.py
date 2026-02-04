from __future__ import annotations

from collections.abc import Sequence

from customer_dedupe.models import CustomerRecord, MatchCandidate
from customer_dedupe.schema import FieldTag, RecordSchema


class NameFuzzyMatcher:
    """Simple deterministic matcher: candidate if names differ by <= 1 edit."""

    def __init__(
        self,
        schema: RecordSchema,
        name_tag: FieldTag = FieldTag.NAME,
        max_edits: int = 1,
        score: float = 0.7,
    ) -> None:
        self._schema = schema
        self._name_tag = name_tag
        self._max_edits = max_edits
        self._score = score

    def match(self, records: Sequence[CustomerRecord]) -> list[MatchCandidate]:
        candidates: list[MatchCandidate] = []
        for i, left in enumerate(records):
            left_name = self._schema.joined_value(left.attributes, self._name_tag).lower()
            if not left_name:
                continue
            for right in records[i + 1 :]:
                right_name = self._schema.joined_value(right.attributes, self._name_tag).lower()
                if not right_name:
                    continue
                if _levenshtein(left_name, right_name) <= self._max_edits:
                    candidates.append(
                        MatchCandidate(
                            left_id=left.record_id,
                            right_id=right.record_id,
                            score=self._score,
                            metadata={"rule": "name_levenshtein", "tag": self._name_tag.value},
                        )
                    )
        return candidates


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    prev = list(range(len(right) + 1))
    for i, c1 in enumerate(left, start=1):
        curr = [i]
        for j, c2 in enumerate(right, start=1):
            cost = 0 if c1 == c2 else 1
            curr.append(min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]
