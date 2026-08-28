from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Mapping

from services.worker.contracts import MessageDisposition, WorkerMessageProcessor
from services.worker.processor import InvalidWorkerMessage


logger = logging.getLogger(__name__)
_processor: WorkerMessageProcessor | None = None
_processor_lock = Lock()


def _default_processor() -> WorkerMessageProcessor:
    global _processor
    if _processor is not None:
        return _processor
    with _processor_lock:
        if _processor is None:
            from services.worker.composition import build_default_processor

            _processor = build_default_processor()
    return _processor


def handler(
    event: Mapping[str, Any],
    context: Any,
    *,
    processor: WorkerMessageProcessor | None = None,
) -> dict[str, list[dict[str, str]]]:
    """Process SQS records using Lambda's partial-batch response contract."""

    del context
    records = event.get("Records", [])
    if not isinstance(records, list):
        records = []
    if not records:
        return {"batchItemFailures": []}
    selected_processor = processor or _default_processor()
    failures: list[dict[str, str]] = []
    seen_failures: set[str] = set()
    for record in records:
        if not isinstance(record, Mapping):
            logger.warning("Discarding an invalid SQS record")
            continue
        message_id_value = record.get("messageId")
        message_id = (
            message_id_value.strip()
            if isinstance(message_id_value, str) and message_id_value.strip()
            else ""
        )
        if not message_id:
            logger.warning("Discarding an SQS record without a message id")
            continue
        try:
            disposition = selected_processor.process_message(
                message_id=message_id,
                body=record.get("body", ""),
            )
        except InvalidWorkerMessage:
            # Malformed messages are permanent and should not consume DLQ retries.
            logger.warning("Discarding malformed enrichment message id=%s", message_id)
            continue
        except Exception as exc:
            # Exception text may come from a dependency. Log only its type so a
            # credential embedded in an unexpected URL can never reach logs.
            logger.error(
                "Enrichment message failed unexpectedly id=%s errorType=%s",
                message_id,
                type(exc).__name__,
            )
            disposition = MessageDisposition.RETRY
        if disposition is MessageDisposition.RETRY and message_id not in seen_failures:
            failures.append({"itemIdentifier": message_id})
            seen_failures.add(message_id)
    return {"batchItemFailures": failures}


def reset_cached_processor_for_tests() -> None:
    global _processor
    with _processor_lock:
        _processor = None
