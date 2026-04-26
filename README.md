# Customer Dedupe

Initial Python interfaces for a 3-stage deduplication pipeline:

1. Column cleanup
2. Deterministic matching
3. Embedding-based matching and clustering

The code is column-agnostic through semantic tags (`NAME`, `ADDRESS`, `EMAIL`, etc.) mapped via `RecordSchema`.

## Quick start

```python
from customer_dedupe.datasets import RETAIL_COLUMNS, RETAIL_SCHEMA, ReferenceDatasetGenerator
from customer_dedupe.runners import LocalDedupePipeline
from customer_dedupe.schema import FieldTag
from customer_dedupe.steps import (
    BruteForceVectorIndex,
    DefaultEmbeddingMatcher,
    FunctionalCleaner,
    NameFuzzyMatcher,
    SimpleTextEmbeddingModel,
)

records = ReferenceDatasetGenerator(seed=42).generate(
    columns=RETAIL_COLUMNS,
    schema=RETAIL_SCHEMA,
    size=1000,
)

cleaner = FunctionalCleaner(
    schema=RETAIL_SCHEMA,
    tag_transforms={
        FieldTag.POSTCODE: lambda v: v.replace(" ", "").upper(),
        FieldTag.ADDRESS: lambda v: " ".join(v.lower().split()),
    },
)
embedding_matcher = DefaultEmbeddingMatcher(
    embedding_model=SimpleTextEmbeddingModel(
        schema=RETAIL_SCHEMA,
        tags=[FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL],
    ),
    vector_index=BruteForceVectorIndex(),
    similarity_threshold=0.88,
)

pipeline = LocalDedupePipeline(
    cleaner=cleaner,
    deterministic_matcher=NameFuzzyMatcher(schema=RETAIL_SCHEMA, max_edits=1),
    embedding_matcher=embedding_matcher,
)

clusters = pipeline.run(records)
print(f"clusters: {len(clusters)}")
```

## Notes

- `BruteForceVectorIndex` is intentionally simple for local development and testing.
- `DataflowDedupePipeline` is a stable interface stub for a future Beam/Dataflow implementation.
- `ReferenceDatasetGenerator` produces synthetic rows with controlled duplicate injection.
- Provided `RETAIL_COLUMNS` and `RETAIL_SCHEMA` are based on your source column layout.

## CLI

Recommended install (includes SBERT, the higher-accuracy embedding backend):

```bash
./scripts/install.sh --sbert
```

Hashing-only install (no neural embedding deps; faster setup, lower recall):

```bash
./scripts/install.sh
```

Run test end-to-end. With the SBERT install above this is all you need — the
CLI auto-detects `sentence-transformers` and uses it; otherwise it falls back
to the hashing backend:

```bash
customer-dedupe run-test --size 2000 --output-dir data/cli_output
```

The chosen backend is printed at the start of the run (`Embedding backend: sbert`
or `... hashing`). Force a specific backend with `--embedding-backend {auto,hashing,sbert}`.

On a 1000-record synthetic benchmark (seed=42, 15% duplicate rate, threshold 0.85)
SBERT recovered 99.4% of true duplicate pairs vs 81.0% for the hashing baseline,
with both backends scoring 1.0 precision. SBERT is the recommended default for
any meaningful dataset.

Outputs:

- `data/cli_output/test_dataset.csv`
- `data/cli_output/clusters.json`
- `data/cli_output/summary.json`
