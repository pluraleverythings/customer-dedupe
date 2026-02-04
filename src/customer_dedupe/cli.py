from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from customer_dedupe.datasets import RETAIL_COLUMNS, RETAIL_SCHEMA, ReferenceDatasetGenerator
from customer_dedupe.models import CustomerRecord
from customer_dedupe.schema import FieldTag
from customer_dedupe.steps import (
    BruteForceVectorIndex,
    DefaultEmbeddingMatcher,
    FunctionalCleaner,
    NameFuzzyMatcher,
    SbertEmbeddingModel,
    SimpleTextEmbeddingModel,
)

_NOISY_PREVIEW_COLUMNS = {
    "CUSTOMER_PK",
    "DIM_CUSTOMER_ID",
    "DIM_INDIVIDUAL_ID",
    "WEB_CUSTOMER_ID",
    "REGISTERED_DATE",
    "LAST_UPDATED",
    "GDPR_REGISTERED_DATE",
    "ANONYMISED",
    "GDPR_ANONYMISED",
    "SOURCE",
}


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "run-test":
        run_test(
            size=args.size,
            duplicate_rate=args.duplicate_rate,
            seed=args.seed,
            output_dir=args.output_dir,
            input_csv=args.input_csv,
            similarity_threshold=args.similarity_threshold,
            embedding_backend=args.embedding_backend,
            sbert_model=args.sbert_model,
            sbert_batch_size=args.sbert_batch_size,
            show_clusters=args.show_clusters,
            email_constraint=args.email_constraint,
        )
        return

    parser.print_help()


def run_test(
    *,
    size: int,
    duplicate_rate: float,
    seed: int,
    output_dir: Path,
    input_csv: Path | None,
    similarity_threshold: float,
    embedding_backend: str,
    sbert_model: str,
    sbert_batch_size: int,
    show_clusters: int,
    email_constraint: str,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if input_csv is None:
        records = ReferenceDatasetGenerator(seed=seed).generate(
            columns=RETAIL_COLUMNS,
            schema=RETAIL_SCHEMA,
            size=size,
            duplicate_rate=duplicate_rate,
        )
        dataset_path = output_dir / "test_dataset.csv"
        _write_records_csv(dataset_path, records, RETAIL_COLUMNS)
    else:
        records = _read_records_csv(input_csv)
        dataset_path = input_csv

    cleaner = FunctionalCleaner(
        schema=RETAIL_SCHEMA,
        tag_transforms={
            FieldTag.POSTCODE: lambda value: value.replace(" ", "").upper(),
            FieldTag.ADDRESS: lambda value: " ".join(value.lower().split()),
            FieldTag.EMAIL: lambda value: value.strip().lower(),
        },
    )
    deterministic_matcher = NameFuzzyMatcher(schema=RETAIL_SCHEMA, max_edits=1)
    if embedding_backend == "sbert":
        embedding_model = SbertEmbeddingModel(
            schema=RETAIL_SCHEMA,
            tags=[FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL],
            model_name=sbert_model,
            batch_size=sbert_batch_size,
        )
    else:
        embedding_model = SimpleTextEmbeddingModel(
            schema=RETAIL_SCHEMA,
            tags=[FieldTag.NAME, FieldTag.ADDRESS, FieldTag.EMAIL],
        )

    embedding_matcher = DefaultEmbeddingMatcher(
        embedding_model=embedding_model,
        vector_index=BruteForceVectorIndex(),
        similarity_threshold=similarity_threshold,
    )

    cleaned_records = cleaner.clean(records)
    deterministic_candidates = deterministic_matcher.match(cleaned_records)
    embedding_candidates = embedding_matcher.match(cleaned_records)
    all_candidates = deterministic_candidates + embedding_candidates
    constrained_candidates = _apply_email_constraint(
        candidates=all_candidates,
        records=cleaned_records,
        constraint=email_constraint,
    )
    clusters = embedding_matcher.cluster(constrained_candidates)

    clusters_path = output_dir / "clusters.json"
    summary_path = output_dir / "summary.json"

    _write_json(clusters_path, [asdict(cluster) for cluster in clusters])
    summary = _build_summary(
        record_count=len(cleaned_records),
        deterministic_candidates=len(deterministic_candidates),
        embedding_candidates=len(embedding_candidates),
        constrained_candidates=len(constrained_candidates),
        clusters=clusters,
        dataset_path=dataset_path,
        clusters_path=clusters_path,
    )
    _write_json(summary_path, summary)

    print(f"Dataset: {dataset_path}")
    print(f"Clusters: {clusters_path}")
    print(f"Summary: {summary_path}")
    print("---")
    print(f"records={summary['record_count']}")
    print(f"candidate_pairs={summary['candidate_pair_count']}")
    print(f"candidate_pairs_after_email_constraint={summary['candidate_pair_count_after_email_constraint']}")
    print(f"clusters={summary['cluster_count']}")
    print(f"clustered_records={summary['clustered_record_count']}")
    print(f"avg_cluster_size={summary['avg_cluster_size']}")
    if show_clusters > 0:
        print("---")
        print("sample_clusters=")
        print(json.dumps(_cluster_sample_payload(clusters, records, limit=show_clusters), indent=2))


def _build_summary(
    *,
    record_count: int,
    deterministic_candidates: int,
    embedding_candidates: int,
    constrained_candidates: int,
    clusters: list,
    dataset_path: Path,
    clusters_path: Path,
) -> dict[str, object]:
    cluster_sizes = [len(cluster.record_ids) for cluster in clusters]
    clustered_record_count = len({record_id for cluster in clusters for record_id in cluster.record_ids})

    return {
        "record_count": record_count,
        "deterministic_candidate_count": deterministic_candidates,
        "embedding_candidate_count": embedding_candidates,
        "candidate_pair_count": deterministic_candidates + embedding_candidates,
        "candidate_pair_count_after_email_constraint": constrained_candidates,
        "cluster_count": len(clusters),
        "clustered_record_count": clustered_record_count,
        "avg_cluster_size": round(sum(cluster_sizes) / len(cluster_sizes), 3) if cluster_sizes else 0.0,
        "max_cluster_size": max(cluster_sizes) if cluster_sizes else 0,
        "min_cluster_size": min(cluster_sizes) if cluster_sizes else 0,
        "dataset_path": str(dataset_path),
        "clusters_path": str(clusters_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="customer-dedupe", description="Customer dedupe CLI")
    subparsers = parser.add_subparsers(dest="command")

    run_test_parser = subparsers.add_parser(
        "run-test",
        help="Generate or load a test dataset, run dedupe, and output clusters + summary",
    )
    run_test_parser.add_argument("--size", type=int, default=2000)
    run_test_parser.add_argument("--duplicate-rate", type=float, default=0.15)
    run_test_parser.add_argument("--seed", type=int, default=42)
    run_test_parser.add_argument("--similarity-threshold", type=float, default=0.95)
    run_test_parser.add_argument("--input-csv", type=Path, default=None)
    run_test_parser.add_argument("--output-dir", type=Path, default=Path("data/cli_output"))
    run_test_parser.add_argument("--embedding-backend", choices=["hashing", "sbert"], default="hashing")
    run_test_parser.add_argument("--sbert-model", type=str, default="all-MiniLM-L6-v2")
    run_test_parser.add_argument("--sbert-batch-size", type=int, default=64)
    run_test_parser.add_argument("--show-clusters", type=int, default=10)
    run_test_parser.add_argument("--email-constraint", choices=["none", "canonical"], default="canonical")

    return parser


def _write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _write_records_csv(path: Path, records: list[CustomerRecord], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["RECORD_ID", *columns])
        writer.writeheader()
        for record in records:
            writer.writerow({"RECORD_ID": record.record_id, **record.attributes})


def _read_records_csv(path: Path) -> list[CustomerRecord]:
    records: list[CustomerRecord] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            record_id = row.get("RECORD_ID")
            if not record_id:
                continue
            attrs = {k: v for k, v in row.items() if k != "RECORD_ID"}
            records.append(CustomerRecord(record_id=record_id, attributes=attrs))
    return records


def _cluster_sample_payload(
    clusters: list,
    records: list[CustomerRecord],
    limit: int = 10,
) -> list[dict[str, Any]]:
    by_id = {record.record_id: record for record in records}
    ranked = sorted(clusters, key=lambda cluster: (-len(cluster.record_ids), cluster.cluster_id))
    payload: list[dict[str, Any]] = []

    for cluster in ranked[:limit]:
        cluster_records = [by_id[record_id] for record_id in cluster.record_ids if record_id in by_id]
        differing_columns = _differing_columns(cluster_records)
        projected_records: list[dict[str, Any]] = []
        for record in cluster_records:
            projected_values = [str(record.attributes.get(column, "")) for column in differing_columns]
            projected_records.append(
                {
                    "record_id": record.record_id,
                    "projected_values": projected_values,
                }
            )

        payload.append(
            {
                "cluster_id": cluster.cluster_id,
                "size": len(cluster.record_ids),
                "confidence": round(cluster.confidence, 4),
                "differing_columns": differing_columns,
                "projected_records": projected_records,
            }
        )
    return payload


def _differing_columns(records: list[CustomerRecord]) -> list[str]:
    if len(records) <= 1:
        return []
    columns = sorted({column for record in records for column in record.attributes})
    differing: list[str] = []
    for column in columns:
        if column.upper() in _NOISY_PREVIEW_COLUMNS:
            continue
        values = {str(record.attributes.get(column, "")).strip() for record in records}
        if len(values) > 1:
            differing.append(column)
    return differing


def _apply_email_constraint(
    *,
    candidates: list,
    records: list[CustomerRecord],
    constraint: str,
) -> list:
    if constraint == "none":
        return candidates

    emails_by_id: dict[str, str] = {}
    for record in records:
        raw_email = RETAIL_SCHEMA.joined_value(record.attributes, FieldTag.EMAIL)
        emails_by_id[record.record_id] = _canonical_email(raw_email)

    filtered = []
    for candidate in candidates:
        left_email = emails_by_id.get(candidate.left_id, "")
        right_email = emails_by_id.get(candidate.right_id, "")
        if _emails_compatible(left_email, right_email):
            filtered.append(candidate)
    return filtered


def _canonical_email(email: str) -> str:
    email = email.strip().lower()
    if "@" not in email:
        return email
    local, domain = email.split("@", maxsplit=1)
    local = local.split("+", maxsplit=1)[0]
    if domain in {"gmail.com", "googlemail.com"}:
        local = local.replace(".", "")
    return f"{local}@{domain}"


def _emails_compatible(left: str, right: str) -> bool:
    if not left or not right:
        return True
    return left == right


if __name__ == "__main__":
    main()
