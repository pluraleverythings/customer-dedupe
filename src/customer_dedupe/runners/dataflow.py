from __future__ import annotations

from collections.abc import Sequence

from customer_dedupe.models import Cluster, CustomerRecord


class DataflowDedupePipeline:
    """Stub interface for cloud-distributed execution.

    Keep this shape stable while we replace internals with Beam/Dataflow transforms.
    """

    def __init__(self, job_name: str, temp_location: str) -> None:
        self.job_name = job_name
        self.temp_location = temp_location

    def run(self, records: Sequence[CustomerRecord]) -> list[Cluster]:
        raise NotImplementedError(
            "Dataflow execution is not implemented yet. "
            "Use this interface to preserve compatibility with a future Beam runner."
        )
