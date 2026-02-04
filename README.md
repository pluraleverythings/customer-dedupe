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

Easy install:

```bash
./scripts/install.sh
```

With SBERT dependencies:

```bash
./scripts/install.sh --sbert
```

Run test end-to-end (generate dataset, dedupe, output results):

```bash
PYTHONPATH=src python3 -m customer_dedupe run-test --size 2000 --output-dir data/cli_output
```

Or after installing package entrypoints:

```bash
customer-dedupe run-test --size 2000 --output-dir data/cli_output
```

Use SBERT embeddings:

```bash
pip install sentence-transformers
PYTHONPATH=src python3 -m customer_dedupe run-test \
  --size 2000 \
  --embedding-backend sbert \
  --sbert-model all-MiniLM-L6-v2 \
  --output-dir data/cli_output
```

Outputs:

- `data/cli_output/test_dataset.csv`
- `data/cli_output/clusters.json`
- `data/cli_output/summary.json`
