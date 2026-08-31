from __future__ import annotations

import logging
from threading import Lock
from typing import Any, Mapping

from services.bulk.composition import InvalidBulkMessage
from services.bulk.processor import BulkMessageDisposition


logger = logging.getLogger(__name__)
_processor: Any | None = None
_processor_lock = Lock()


def _default_processor():
    global _processor
    if _processor is not None:
        return _processor
    with _processor_lock:
        if _processor is None:
            from services.bulk.composition import build_default_processor

            _processor = build_default_processor()
    return _processor


def handler(
    event: Mapping[str, Any],
    context: Any,
    *,
    processor: Any | None = None,
) -> dict[str, list[dict[str, str]]]:
    """Handle BulkManifest and RetryManifestImports with partial SQS failures."""

    del context
    records = event.get("Records", [])
    if not isinstance(records, list):
        records = []
    selected = processor or _default_processor()
    failures: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, Mapping):
            continue
        raw_id = record.get("messageId")
        message_id = raw_id.strip() if isinstance(raw_id, str) else ""
        if not message_id:
            continue
        try:
            disposition = selected.process_message(
                message_id=message_id,
                body=record.get("body", ""),
            )
        except InvalidBulkMessage:
            logger.warning("Discarding malformed bulk message id=%s", message_id)
            continue
        except Exception as exc:
            logger.error(
                "Bulk message failed unexpectedly id=%s errorType=%s",
                message_id,
                type(exc).__name__,
            )
            disposition = BulkMessageDisposition.RETRY
        if disposition is BulkMessageDisposition.RETRY and message_id not in seen:
            failures.append({"itemIdentifier": message_id})
            seen.add(message_id)
    return {"batchItemFailures": failures}


def reset_cached_processor_for_tests() -> None:
    global _processor
    with _processor_lock:
        _processor = None
