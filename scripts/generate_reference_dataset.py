from __future__ import annotations

import argparse
import csv
from pathlib import Path

from customer_dedupe.datasets import RETAIL_COLUMNS, RETAIL_SCHEMA, ReferenceDatasetGenerator


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic retail customer dataset")
    parser.add_argument("--size", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duplicate-rate", type=float, default=0.15)
    parser.add_argument("--output", type=Path, default=Path("data/reference_retail_customers.csv"))
    args = parser.parse_args()

    records = ReferenceDatasetGenerator(seed=args.seed).generate(
        columns=RETAIL_COLUMNS,
        schema=RETAIL_SCHEMA,
        size=args.size,
        duplicate_rate=args.duplicate_rate,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["RECORD_ID", *RETAIL_COLUMNS])
        writer.writeheader()
        for record in records:
            writer.writerow({"RECORD_ID": record.record_id, **record.attributes})


if __name__ == "__main__":
    main()
