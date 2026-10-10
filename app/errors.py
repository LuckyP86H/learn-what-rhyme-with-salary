"""The error taxonomy that drives retry decisions.

Celery has no opinion about which errors are worth retrying; you have to classify them yourself.
Getting this wrong is how you either lose work (retrying nothing) or melt a broker (retrying everything).
"""


class TransientError(Exception):
    """Might work next time (upstream timeout, soft time limit): retry with exponential backoff."""


class PermanentError(Exception):
    """Will never work (corrupt audio, invalid metadata): do not retry, send the message to the DLQ."""


class LeaseLost(Exception):
    """Another worker took this chunk over while we were running (we were presumed dead)."""
