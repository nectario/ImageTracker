from __future__ import annotations

import base64
import json
from typing import Any, Sequence

from services.domain.errors import InvalidCursorError


class CursorCodec:
    """Versioned opaque cursor codec.

    Cursors intentionally contain only non-sensitive sort positions. Ownership
    remains enforced independently in every query, so changing a cursor cannot
    reveal another user's records.
    """

    VERSION = 1

    def encode(self, kind: str, values: Sequence[Any]) -> str:
        raw = json.dumps(
            {"v": self.VERSION, "k": kind, "p": list(values)},
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    def decode(self, cursor: str | None, *, kind: str) -> tuple[Any, ...] | None:
        if cursor is None:
            return None
        try:
            encoded = cursor.encode("ascii")
            padded = encoded + b"=" * (-len(encoded) % 4)
            payload = json.loads(base64.b64decode(padded, altchars=b"-_", validate=True))
        except (UnicodeEncodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise InvalidCursorError() from exc
        if (
            not isinstance(payload, dict)
            or payload.get("v") != self.VERSION
            or payload.get("k") != kind
            or not isinstance(payload.get("p"), list)
        ):
            raise InvalidCursorError()
        return tuple(payload["p"])
