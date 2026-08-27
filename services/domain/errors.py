from __future__ import annotations


class DomainError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class NotFoundError(DomainError):
    pass


class ForbiddenError(DomainError):
    pass


class ConflictError(DomainError):
    pass


class InvalidCursorError(DomainError):
    def __init__(self, detail: str = "The continuation cursor is invalid") -> None:
        super().__init__("InvalidCursor", detail)


class RetryNotAllowedError(ConflictError):
    pass
