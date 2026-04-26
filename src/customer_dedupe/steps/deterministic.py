from __future__ import annotations

import unicodedata
from collections.abc import Sequence

from customer_dedupe.models import CustomerRecord, MatchCandidate
from customer_dedupe.schema import FieldTag, RecordSchema

_TITLE_TOKENS = frozenset(
    {"mr", "mrs", "ms", "miss", "mx", "dr", "prof", "professor", "sir", "lord", "lady", "rev"}
)


class NameFuzzyMatcher:
    """Token-set name matcher: candidate if every token in the smaller name set has a
    same-or-near match in the other set within ``max_edits`` Levenshtein distance.

    Compared to a single joined-string Levenshtein this is robust against title swaps,
    first/last reordering, and asymmetric population of duplicated NAME columns.
    """

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
        prepared = [
            (record.record_id, _name_tokens(self._schema, record.attributes, self._name_tag))
            for record in records
        ]
        candidates: list[MatchCandidate] = []
        for i, (left_id, left_tokens) in enumerate(prepared):
            if not left_tokens:
                continue
            for right_id, right_tokens in prepared[i + 1 :]:
                if not right_tokens:
                    continue
                if _token_sets_match(left_tokens, right_tokens, self._max_edits):
                    candidates.append(
                        MatchCandidate(
                            left_id=left_id,
                            right_id=right_id,
                            score=self._score,
                            metadata={"rule": "name_token_set", "tag": self._name_tag.value},
                        )
                    )
        return candidates


def _name_tokens(schema: RecordSchema, attrs, tag: FieldTag) -> frozenset[str]:
    raw = schema.joined_value(attrs, tag).lower()
    raw = "".join(ch for ch in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(ch))
    return frozenset(tok for tok in raw.split() if tok and tok not in _TITLE_TOKENS)


def _token_sets_match(left: frozenset[str], right: frozenset[str], max_edits: int) -> bool:
    smaller, larger = (left, right) if len(left) <= len(right) else (right, left)
    used: set[str] = set()
    for token in smaller:
        match = None
        for candidate in larger:
            if candidate in used:
                continue
            if _levenshtein(token, candidate) <= max_edits:
                match = candidate
                break
        if match is None:
            return False
        used.add(match)
    return True


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
