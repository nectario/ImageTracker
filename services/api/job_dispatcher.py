from __future__ import annotations

import json
from typing import Any
from uuid import UUID


class JobDispatchError(RuntimeError):
    """A durable job could not be published to the processing queue."""


class SqsJobDispatcher:
    """Publish post-commit job identities in bounded SQS batches."""

    def __init__(self, *, client: Any, queue_url: str, delay_seconds: int = 2) -> None:
        if not queue_url:
            raise ValueError("The processing queue URL is required")
        if not 0 <= delay_seconds <= 900:
            raise ValueError("SQS delay must be between 0 and 900 seconds")
        self._client = client
        self._queue_url = queue_url
        self._delay_seconds = delay_seconds

    def dispatch(self, *, job_ids: tuple[UUID, ...], job_type: str) -> None:
        if not job_ids:
            return
        for offset in range(0, len(job_ids), 10):
            batch = job_ids[offset : offset + 10]
            entries = [
                {
                    "Id": str(index),
                    "MessageBody": json.dumps(
                        {"jobId": str(job_id), "jobType": job_type},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    "DelaySeconds": self._delay_seconds,
                }
                for index, job_id in enumerate(batch)
            ]
            response = self._client.send_message_batch(
                QueueUrl=self._queue_url,
                Entries=entries,
            )
            failed = response.get("Failed") or []
            if failed:
                codes = sorted(
                    {str(item.get("Code") or "Unknown") for item in failed}
                )
                raise JobDispatchError(
                    f"SQS rejected {len(failed)} processing jobs: {', '.join(codes)}"
                )
