from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from math import sqrt

from customer_dedupe.interfaces import EmbeddingModel, VectorIndex
from customer_dedupe.models import Cluster, CustomerRecord, MatchCandidate
from customer_dedupe.schema import FieldTag, RecordSchema


class SimpleTextEmbeddingModel:
    """Hashing-based baseline embedding model for local testing.

    Replace with a production model adapter (OpenAI, Vertex, sentence-transformers, etc).
    """

    def __init__(self, schema: RecordSchema, tags: Sequence[FieldTag], dimensions: int = 64) -> None:
        self._schema = schema
        self._tags = list(tags)
        self._dimensions = dimensions

    def embed(self, records: Sequence[CustomerRecord]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for record in records:
            vector = [0.0] * self._dimensions
            text_parts = [self._schema.joined_value(record.attributes, tag) for tag in self._tags]
            text = " ".join(part.lower() for part in text_parts if part).strip()
            for token in text.split():
                idx = hash(token) % self._dimensions
                vector[idx] += 1.0
            vectors.append(_l2_normalize(vector))
        return vectors


class SbertEmbeddingModel:
    """Sentence-Transformers embedding adapter (SBERT)."""

    def __init__(
        self,
        schema: RecordSchema,
        tags: Sequence[FieldTag],
        model_name: str = "all-MiniLM-L6-v2",
        batch_size: int = 64,
    ) -> None:
        self._schema = schema
        self._tags = list(tags)
        self._batch_size = batch_size
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "SBERT backend requires sentence-transformers. "
                "Install with: pip install sentence-transformers"
            ) from exc
        self._model = SentenceTransformer(model_name)

    def embed(self, records: Sequence[CustomerRecord]) -> list[list[float]]:
        texts: list[str] = []
        for record in records:
            text_parts = [self._schema.joined_value(record.attributes, tag) for tag in self._tags]
            texts.append(" ".join(part.lower() for part in text_parts if part).strip())
        vectors = self._model.encode(
            texts,
            batch_size=self._batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [vector.tolist() for vector in vectors]


class BruteForceVectorIndex:
    """Local reference index.

    Keeps interfaces clean so this can later be swapped for FAISS/ScaNN/ANN+Dataflow.
    """

    def __init__(self) -> None:
        self._record_ids: list[str] = []
        self._vectors: list[list[float]] = []

    def build(self, record_ids: Sequence[str], vectors: Sequence[Sequence[float]]) -> None:
        self._record_ids = list(record_ids)
        self._vectors = [list(v) for v in vectors]

    def query_similar_pairs(self, min_similarity: float) -> list[MatchCandidate]:
        pairs: list[MatchCandidate] = []
        for i, left in enumerate(self._vectors):
            for j in range(i + 1, len(self._vectors)):
                score = _dot(left, self._vectors[j])
                if score >= min_similarity:
                    pairs.append(
                        MatchCandidate(
                            left_id=self._record_ids[i],
                            right_id=self._record_ids[j],
                            score=score,
                            metadata={"source": "embedding"},
                        )
                    )
        return pairs


class DefaultEmbeddingMatcher:
    def __init__(
        self,
        embedding_model: EmbeddingModel,
        vector_index: VectorIndex,
        similarity_threshold: float = 0.85,
    ) -> None:
        self._embedding_model = embedding_model
        self._vector_index = vector_index
        self._similarity_threshold = similarity_threshold

    def match(self, records: Sequence[CustomerRecord]) -> list[MatchCandidate]:
        vectors = self._embedding_model.embed(records)
        ids = [r.record_id for r in records]
        self._vector_index.build(ids, vectors)
        return self._vector_index.query_similar_pairs(self._similarity_threshold)

    def cluster(self, candidates: Sequence[MatchCandidate]) -> list[Cluster]:
        if not candidates:
            return []

        uf = _UnionFind()
        score_map: dict[str, list[float]] = defaultdict(list)

        for candidate in candidates:
            uf.union(candidate.left_id, candidate.right_id)

        for candidate in candidates:
            root = uf.find(candidate.left_id)
            score_map[root].append(candidate.score)

        groups = uf.groups()
        clusters: list[Cluster] = []
        for root, members in groups.items():
            if len(members) < 2:
                continue
            scores = score_map.get(root, [0.0])
            clusters.append(
                Cluster(
                    cluster_id=f"cluster_{root}",
                    record_ids=sorted(members),
                    confidence=sum(scores) / len(scores),
                )
            )
        return sorted(clusters, key=lambda c: c.cluster_id)


class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        if item not in self._parent:
            self._parent[item] = item
            return item
        if self._parent[item] != item:
            self._parent[item] = self.find(self._parent[item])
        return self._parent[item]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left != root_right:
            self._parent[root_right] = root_left

    def groups(self) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = defaultdict(list)
        for item in list(self._parent):
            grouped[self.find(item)].append(item)
        return grouped


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _l2_normalize(vector: Sequence[float]) -> list[float]:
    norm = sqrt(sum(v * v for v in vector))
    if norm == 0:
        return [0.0] * len(vector)
    return [v / norm for v in vector]
