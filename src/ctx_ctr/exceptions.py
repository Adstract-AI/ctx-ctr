"""Project-specific exceptions."""


class CtxCtrError(Exception):
    """Base exception for expected project failures."""


class SeedError(CtxCtrError):
    """Raised when demo seed data cannot be prepared or written."""

