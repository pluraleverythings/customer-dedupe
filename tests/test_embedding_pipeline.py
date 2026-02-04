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


def test_pipeline_returns_cluster_for_near_duplicates() -> None:
    schema = RecordSchema.from_mapping(
        {
            FieldTag.NAME: ["FIRSTNAME", "LASTNAME"],
            FieldTag.ADDRESS: ["BILLING_ADDRESS_LINE1"],
            FieldTag.EMAIL: ["EMAIL"],
        }
    )

    records = [
        CustomerRecord(
            record_id="1",
            attributes={
                "FIRSTNAME": "Jane",
                "LASTNAME": "Smith",
                "BILLING_ADDRESS_LINE1": "12 Market Street",
                "EMAIL": "jane@example.com",
            },
        ),
        CustomerRecord(
            record_id="2",
            attributes={
                "FIRSTNAME": "Jane",
                "LASTNAME": "Smit",
                "BILLING_ADDRESS_LINE1": "12 Market St",
                "EMAIL": "jane@example.com",
            },
        ),
        CustomerRecord(
            record_id="3",
            attributes={
                "FIRSTNAME": "Alex",
                "LASTNAME": "Doe",
                "BILLING_ADDRESS_LINE1": "44 Pine Road",
                "EMAIL": "alex@example.com",
            },
        ),
    ]

    pipeline = LocalDedupePipeline(
        cleaner=FunctionalCleaner(schema=schema),
        deterministic_matcher=NameFuzzyMatcher(schema=schema, max_edits=1),
        embedding_matcher=DefaultEmbeddingMatcher(
            embedding_model=SimpleTextEmbeddingModel(
                schema=schema,
                tags=[FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL],
            ),
            vector_index=BruteForceVectorIndex(),
            similarity_threshold=0.6,
        ),
    )

    clusters = pipeline.run(records)
    clustered_ids = [set(c.record_ids) for c in clusters]

    assert {"1", "2"} in clustered_ids
