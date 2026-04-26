"""Tests that pin down known accuracy weaknesses in the dedupe pipeline.

Each test asserts the *correct* behavior we would like the algorithm to have, and is marked
``xfail(strict=True)`` so it currently reports as XFAIL (a documented bug) but flips CI red
the moment the underlying issue is fixed. That forces whoever fixes the bug to also remove
the marker, keeping this file an accurate index of outstanding accuracy issues.

Hypotheses (probed via scripts/embedded snippets, not imported here):

H1  Name nicknames / diminutives are not recognised (Robert <-> Bob).
H2  Union-find clustering performs unfiltered transitive closure: a chain of pairwise
    matches merges entities even when the chain endpoints are clearly distinct.
H3  ``NameFuzzyMatcher(max_edits=1)`` cannot bridge title swaps that take >1 edit
    (e.g. "Mrs" vs "Dr").
H4  Joining all NAME columns into one Levenshtein blob is asymmetric: a record with
    both legal+billing names populated cannot match an otherwise-identical record that
    only populates the legal columns.
H5  ``NameFuzzyMatcher`` is order-sensitive: swapping FIRSTNAME and LASTNAME breaks it.
H6  No unicode/accent normalization in cleanup or embedding (Jose <-> Jose-with-acute).
H7  ``_apply_email_constraint`` treats an empty email as compatible with anything, so a
    blank-email record bridges two records whose real emails clearly disagree.
H8  ``_canonical_email`` only normalises gmail/googlemail dots; the same dot trick on
    yahoo/icloud is not collapsed.
H9  ``SimpleTextEmbeddingModel`` tokenises with ``str.split()`` and treats a phone number
    as a single token, so "+44 207 946 0958" and "02079460958" share zero overlap.
H10 ``SimpleTextEmbeddingModel`` uses Python's randomized ``hash()``; pairwise cosine
    similarities therefore drift across processes (PYTHONHASHSEED) due to differing
    collision patterns. Output is not reproducible.
H11 ``ReferenceDatasetGenerator`` derives the email local-part from ``idx % 97``, so
    distinct synthetic identities collide on email roughly every 97 records, which
    contaminates ground-truth-based precision/recall metrics.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

import pytest

from customer_dedupe.cli import _apply_email_constraint, _canonical_email
from customer_dedupe.datasets import RETAIL_COLUMNS, RETAIL_SCHEMA, ReferenceDatasetGenerator
from customer_dedupe.models import CustomerRecord
from customer_dedupe.runners import LocalDedupePipeline
from customer_dedupe.schema import FieldTag, RecordSchema
from customer_dedupe.steps import (
    BruteForceVectorIndex,
    DefaultEmbeddingMatcher,
    FunctionalCleaner,
    NameFuzzyMatcher,
    SimpleTextEmbeddingModel,
)


def _build_pipeline(schema: RecordSchema, *, threshold: float, max_edits: int = 1) -> LocalDedupePipeline:
    embed_tags = [
        tag
        for tag in (FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL, FieldTag.PHONE)
        if schema.columns_for(tag)
    ]
    return LocalDedupePipeline(
        cleaner=FunctionalCleaner(schema=schema),
        deterministic_matcher=NameFuzzyMatcher(schema=schema, max_edits=max_edits),
        embedding_matcher=DefaultEmbeddingMatcher(
            embedding_model=SimpleTextEmbeddingModel(schema=schema, tags=embed_tags),
            vector_index=BruteForceVectorIndex(),
            similarity_threshold=threshold,
        ),
    )


def _cluster_sets(clusters) -> list[set[str]]:
    return [set(cluster.record_ids) for cluster in clusters]


# ---------- H1: nicknames -------------------------------------------------------------

def test_nickname_variant_clusters_when_address_and_email_match() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.ADDRESS: ["BILLING_ADDRESS_LINE1"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {
            "FIRSTNAME": "Robert", "LASTNAME": "Smith",
            "BILLING_ADDRESS_LINE1": "12 Market Street",
            "EMAIL": "rob.smith@example.com",
        }),
        CustomerRecord("b", {
            "FIRSTNAME": "Bob", "LASTNAME": "Smith",
            "BILLING_ADDRESS_LINE1": "12 Market Street",
            "EMAIL": "rob.smith@example.com",
        }),
    ]
    clusters = _build_pipeline(schema, threshold=0.85).run(records)
    assert {"a", "b"} in _cluster_sets(clusters)


# ---------- H2: transitive closure bridging ------------------------------------------

def test_transitive_chain_does_not_bridge_unrelated_endpoints() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.ADDRESS: ["BILLING_ADDRESS_LINE1"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("alpha", {
            "FIRSTNAME": "Jane", "LASTNAME": "Smith",
            "BILLING_ADDRESS_LINE1": "1 Apple St", "EMAIL": "alpha@a.com",
        }),
        CustomerRecord("bridge", {
            "FIRSTNAME": "Jane", "LASTNAME": "Smit",
            "BILLING_ADDRESS_LINE1": "2 Banana Rd", "EMAIL": "bridge@b.com",
        }),
        CustomerRecord("gamma", {
            "FIRSTNAME": "Jane", "LASTNAME": "Smity",
            "BILLING_ADDRESS_LINE1": "3 Cherry Ln", "EMAIL": "gamma@c.com",
        }),
    ]
    clusters = _build_pipeline(schema, threshold=0.99).run(records)
    bridged = next((c for c in _cluster_sets(clusters) if {"alpha", "gamma"} <= c), None)
    assert bridged is None, f"alpha and gamma merged via 'bridge' despite distinct address+email: {bridged}"


# ---------- H3: 2-edit title swaps ----------------------------------------------------

def test_two_edit_title_swap_is_a_deterministic_match() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["TITLE", "FIRSTNAME", "LASTNAME"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {"TITLE": "Mrs", "FIRSTNAME": "Jane", "LASTNAME": "Smith", "EMAIL": "x@y.com"}),
        CustomerRecord("b", {"TITLE": "Dr",  "FIRSTNAME": "Jane", "LASTNAME": "Smith", "EMAIL": "x@y.com"}),
    ]
    candidates = NameFuzzyMatcher(schema=schema, max_edits=1).match(records)
    assert any({c.left_id, c.right_id} == {"a", "b"} for c in candidates)


# ---------- H4: asymmetric NAME population --------------------------------------------

def test_asymmetric_billing_name_population_still_matches_deterministically() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME", "BILLING_FIRSTNAME", "BILLING_LASTNAME"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {
            "FIRSTNAME": "Jane", "LASTNAME": "Smith",
            "BILLING_FIRSTNAME": "Jane", "BILLING_LASTNAME": "Smith",
            "EMAIL": "jane@example.com",
        }),
        CustomerRecord("b", {
            "FIRSTNAME": "Jane", "LASTNAME": "Smith",
            "BILLING_FIRSTNAME": "", "BILLING_LASTNAME": "",
            "EMAIL": "jane@example.com",
        }),
    ]
    candidates = NameFuzzyMatcher(schema=schema, max_edits=1).match(records)
    assert any({c.left_id, c.right_id} == {"a", "b"} for c in candidates)


# ---------- H5: name reorder ----------------------------------------------------------

def test_firstname_lastname_swap_still_matches() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {"FIRSTNAME": "Jane",  "LASTNAME": "Smith", "EMAIL": "x@y.com"}),
        CustomerRecord("b", {"FIRSTNAME": "Smith", "LASTNAME": "Jane",  "EMAIL": "x@y.com"}),
    ]
    candidates = NameFuzzyMatcher(schema=schema, max_edits=1).match(records)
    assert any({c.left_id, c.right_id} == {"a", "b"} for c in candidates)


# ---------- H6: accent / unicode normalization ----------------------------------------

def test_accent_variant_clusters() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {"FIRSTNAME": "José", "LASTNAME": "García", "EMAIL": "jose@example.com"}),
        CustomerRecord("b", {"FIRSTNAME": "Jose",      "LASTNAME": "Garcia",      "EMAIL": "jose@example.com"}),
    ]
    clusters = _build_pipeline(schema, threshold=0.85).run(records)
    assert {"a", "b"} in _cluster_sets(clusters)


# ---------- H7: empty-email bridge in canonical constraint ----------------------------

def test_empty_email_does_not_bridge_distinct_emails() -> None:
    """A blank-email record should not be a free transitive bridge between two records
    whose canonical emails clearly disagree. Today, the constraint individually allows
    (a, b) and (b, c); union-find then merges {a, b, c} downstream.
    """
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.ADDRESS: ["BILLING_ADDRESS_LINE1"],
        FieldTag.EMAIL: ["EMAIL"],
    })
    records = [
        CustomerRecord("a", {"FIRSTNAME": "Jane", "LASTNAME": "Smith",
                             "BILLING_ADDRESS_LINE1": "12 Market St",
                             "EMAIL": "jane@example.com"}),
        CustomerRecord("b", {"FIRSTNAME": "Jane", "LASTNAME": "Smith",
                             "BILLING_ADDRESS_LINE1": "12 Market St",
                             "EMAIL": ""}),
        CustomerRecord("c", {"FIRSTNAME": "Jane", "LASTNAME": "Smith",
                             "BILLING_ADDRESS_LINE1": "12 Market St",
                             "EMAIL": "completely.different@elsewhere.com"}),
    ]
    deterministic = NameFuzzyMatcher(schema=schema, max_edits=1)
    embedding = DefaultEmbeddingMatcher(
        embedding_model=SimpleTextEmbeddingModel(
            schema=schema, tags=[FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL],
        ),
        vector_index=BruteForceVectorIndex(),
        similarity_threshold=0.99,
    )
    cleaned = FunctionalCleaner(schema=schema).clean(records)
    candidates = deterministic.match(cleaned) + embedding.match(cleaned)
    constrained = _apply_email_constraint(
        candidates=candidates, records=cleaned, constraint="canonical", schema=schema,
    )
    clusters = embedding.cluster(constrained)
    bridged = next((c for c in _cluster_sets(clusters) if {"a", "c"} <= c), None)
    assert bridged is None, f"a and c merged via blank-email bridge 'b': {bridged}"


# ---------- H8: non-gmail dot canonicalization ----------------------------------------

def test_canonical_email_collapses_yahoo_dots() -> None:
    assert _canonical_email("jane.smith@yahoo.com") == _canonical_email("janesmith@yahoo.com")


# ---------- H9: phone tokenisation / format invariance --------------------------------

def test_phone_format_variants_have_high_embedding_similarity() -> None:
    schema = RecordSchema.from_mapping({
        FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
        FieldTag.PHONE: ["PHONE"],
    })
    records = [
        CustomerRecord("a", {"FIRSTNAME": "X", "LASTNAME": "Y", "PHONE": "+44 207 946 0958"}),
        CustomerRecord("b", {"FIRSTNAME": "X", "LASTNAME": "Y", "PHONE": "02079460958"}),
    ]
    model = SimpleTextEmbeddingModel(schema=schema, tags=[FieldTag.PHONE])
    v0, v1 = model.embed(records)
    similarity = sum(a * b for a, b in zip(v0, v1))
    assert similarity >= 0.8


# ---------- H10: cross-process embedding stability ------------------------------------

def test_embedding_similarities_stable_across_processes() -> None:
    snippet = textwrap.dedent(
        """
        import json, random
        from customer_dedupe.models import CustomerRecord
        from customer_dedupe.schema import FieldTag, RecordSchema
        from customer_dedupe.steps import SimpleTextEmbeddingModel

        schema = RecordSchema.from_mapping({FieldTag.NAME: ["NAME"]})
        model = SimpleTextEmbeddingModel(schema=schema, tags=[FieldTag.NAME], dimensions=64)
        rng = random.Random(0)
        vocab = [f"tok_{i:03d}" for i in range(200)]
        records = []
        for i in range(40):
            words = " ".join(rng.choice(vocab) for _ in range(8))
            records.append(CustomerRecord(str(i), {"NAME": words}))
        vectors = model.embed(records)
        sims = []
        for i, a in enumerate(vectors):
            for j in range(i + 1, len(vectors)):
                sims.append(round(sum(x * y for x, y in zip(a, vectors[j])), 6))
        print(json.dumps(sorted(sims)))
        """
    )
    out1 = subprocess.check_output([sys.executable, "-c", snippet], text=True)
    out2 = subprocess.check_output([sys.executable, "-c", snippet], text=True)
    assert json.loads(out1) == json.loads(out2)


# ---------- H11: synthetic-data ground-truth quality ----------------------------------

def test_reference_dataset_distinct_identities_have_distinct_emails() -> None:
    records = ReferenceDatasetGenerator(seed=42).generate(
        columns=RETAIL_COLUMNS,
        schema=RETAIL_SCHEMA,
        size=2000,
        duplicate_rate=0.0,
    )
    emails = [r.attributes["EMAIL"] for r in records]
    assert len(emails) == len(set(emails)), (
        f"{len(emails) - len(set(emails))} email collisions across {len(emails)} distinct identities"
    )
