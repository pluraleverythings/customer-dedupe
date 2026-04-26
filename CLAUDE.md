# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Install

```bash
./scripts/install.sh                # creates .venv and pip install -e .
./scripts/install.sh --sbert        # also installs sentence-transformers
./scripts/install.sh --python python3.11 --venv .venv311   # override interpreter / venv
```

The installer requires Python >= 3.10 and editable-installs the package. SBERT support lives behind the optional `[sbert]` extra in `pyproject.toml`.

### Run the CLI

The package exposes a `run-test` subcommand that generates (or loads) a dataset, runs the full pipeline, and writes `test_dataset.csv`, `clusters.json`, and `summary.json` to `--output-dir`.

```bash
# After ./scripts/install.sh --sbert this auto-selects the SBERT backend.
customer-dedupe run-test --size 2000 --output-dir data/cli_output

# Without install (note PYTHONPATH=src because package lives under src/)
PYTHONPATH=src python3 -m customer_dedupe run-test --size 2000 --output-dir data/cli_output

# Force a specific backend
customer-dedupe run-test --embedding-backend hashing ...
customer-dedupe run-test --embedding-backend sbert --sbert-model all-MiniLM-L6-v2 ...

# Run against an external CSV (schema is auto-inferred from column names; JSON columns are flattened)
customer-dedupe run-test --input-csv path/to/customers.csv --output-dir data/cli_output
```

`--embedding-backend` defaults to `auto`: prefer SBERT if `sentence-transformers` is importable, else fall back to the hashing baseline. The selection is echoed on stdout (`Embedding backend: ...`). SBERT is the recommended default — on the 1000-record synthetic benchmark it lifts recall from 0.81 to 0.99 (precision 1.0 in both cases).

Notable flags: `--similarity-threshold` (default `0.95`), `--duplicate-rate` (synthetic data only, default `0.15`), `--email-constraint {none,canonical}` (default `canonical`, drops candidate pairs whose canonicalized emails disagree), `--show-clusters N` (prints sample clusters to stdout).

### Tests

There is no `pytest` config; tests are plain pytest files under `tests/`.

```bash
.venv/bin/pytest                                # all tests
.venv/bin/pytest tests/test_embedding_pipeline.py::test_pipeline_returns_cluster_for_near_duplicates
PYTHONPATH=src python3 -m pytest                # without an editable install
```

### Generate reference data only

```bash
PYTHONPATH=src python3 scripts/generate_reference_dataset.py --size 10000 --output data/reference_retail_customers.csv
```

## Architecture

### Three-stage pipeline

The dedupe flow is fixed at three stages, glued together by a runner:

1. **Cleanup** (`steps/cleanup.py:FunctionalCleaner`) — normalizes attributes. Supports both raw-column transforms and tag-based transforms; tag transforms apply to every column the schema maps to that tag.
2. **Deterministic matching** (`steps/deterministic.py:NameFuzzyMatcher`) — rule-based candidate pairs (currently Levenshtein <= `max_edits` on joined NAME).
3. **Embedding matching + clustering** (`steps/embedding.py:DefaultEmbeddingMatcher`) — embeds records, queries a `VectorIndex` for similar pairs above a threshold, then unions all candidate pairs (deterministic + embedding) into clusters via union-find. Clusters of size 1 are dropped; cluster confidence is the mean of contributing pair scores.

The CLI layers an extra step on top: after both matchers run, `_apply_email_constraint` filters candidate pairs whose canonicalized emails disagree (gmail dot/plus normalization) before clustering.

### Column-agnostic via semantic tags

The codebase deliberately avoids hard-coding column names. Instead:

- `schema.py:FieldTag` is a `StrEnum` of semantic field kinds (`NAME`, `ADDRESS`, `EMAIL`, `POSTCODE`, ...).
- `schema.py:RecordSchema` is an immutable mapping `FieldTag -> tuple[column_name, ...]`. One tag may cover multiple raw columns (e.g. `NAME` spans `TITLE`, `FIRSTNAME`, `LASTNAME`, `BILLING_FIRSTNAME`, `BILLING_LASTNAME` in `RETAIL_SCHEMA`).
- All steps consume records through `schema.joined_value(attrs, tag)` / `schema.values_for(...)` rather than dict keys directly.
- When adding new logic, take a `RecordSchema` + `FieldTag` rather than a column name.

`datasets/profiles.py` ships a `RETAIL_SCHEMA` / `RETAIL_COLUMNS` pair that mirrors the source data layout. The CLI's `_infer_schema_from_columns` provides a fallback for ad-hoc CSVs by token-matching column names (`name`, `email`, `postcode`, etc.).

### Protocol-driven interfaces

`interfaces.py` defines `typing.Protocol`s — `ColumnCleaner`, `DeterministicMatcher`, `EmbeddingModel`, `VectorIndex`, `EmbeddingMatcher`, `DedupePipeline`. Concrete implementations in `steps/` and `runners/` are duck-typed against these. New backends (real ANN index, production embedding model, etc.) should implement the relevant Protocol; nothing should subclass.

### Runners

- `runners/local.py:LocalDedupePipeline` — wires cleaner + deterministic + embedding matcher for single-machine use; this is the only runner that actually executes today.
- `runners/dataflow.py:DataflowDedupePipeline` — intentional stub that raises `NotImplementedError`. Its `__init__` shape (job_name, temp_location) is the stable contract for a future Beam/Dataflow implementation; preserve this surface when filling it in.

### Embedding backends

- `SimpleTextEmbeddingModel` — token-hash bag-of-words, L2-normalized, default 64 dims. The default; no extra deps.
- `SbertEmbeddingModel` — sentence-transformers wrapper. Imports lazily so the package works without the optional extra installed.
- `BruteForceVectorIndex` — O(N^2) cosine via dot product on already-normalized vectors. Marked as a baseline; swap for FAISS/ScaNN/etc. by implementing `VectorIndex`.

### Models

`models.py` defines the three dataclasses (`@dataclass(slots=True)`) that flow between stages: `CustomerRecord(record_id, attributes)`, `MatchCandidate(left_id, right_id, score, metadata)`, `Cluster(cluster_id, record_ids, confidence)`. `attributes` is an open `dict[str, Any]` — the `RecordSchema` is what gives it structure.

## Conventions

- All modules start with `from __future__ import annotations`; keep that going.
- Public re-exports go through package `__init__.py` files (`customer_dedupe`, `customer_dedupe.steps`, `customer_dedupe.runners`, `customer_dedupe.datasets`). Update `__all__` when adding new public names.
- The package layout uses `src/` (`[tool.setuptools] package-dir = {"" = "src"}`), so anything that runs without an install needs `PYTHONPATH=src`.
- `data/` is gitignored; CLI output and generated datasets live there by default.
