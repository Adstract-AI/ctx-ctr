"""Project-specific exceptions."""


class CtxCtrError(Exception):
    """Base exception for expected project failures."""


class SeedError(CtxCtrError):
    """Raised when demo seed data cannot be prepared or written."""


class CtrStateError(CtxCtrError):
    """Raised when CTR runtime state cannot be read, updated, or written."""

class WeightUpdateError(CtxCtrError):
    """Raised when a Task 2 weight-update run cannot be completed."""
