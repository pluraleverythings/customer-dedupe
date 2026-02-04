"""Customer deduplication interfaces and baseline implementations."""

from customer_dedupe.models import Cluster, CustomerRecord, MatchCandidate
from customer_dedupe.schema import FieldTag, RecordSchema

__all__ = ["Cluster", "CustomerRecord", "MatchCandidate", "FieldTag", "RecordSchema"]
