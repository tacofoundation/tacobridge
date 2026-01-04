"""Exception classes for tacobridge operations."""


class TacoBridgeError(Exception):
    """Base exception for all tacobridge errors."""

    pass


class TacoPlanError(TacoBridgeError):
    """Planning failed before any work started.

    Raised when:
    - Input path does not exist or is not readable
    - SQL query is invalid or references unknown columns
    - Output path already exists
    - Dataset is empty after filtering

    No cleanup needed - nothing was created yet.
    """

    pass


class TacoExecuteError(TacoBridgeError):
    """Single task execution failed.

    Raised when:
    - File read/write fails (permissions, disk full)
    - Network timeout or connection error
    - Remote storage auth failure (S3, GCS, HTTP 403/404)
    - Source file is corrupted or truncated

    Safe to retry the same task. Other tasks unaffected.
    """

    pass


class TacoFinalizeError(TacoBridgeError):
    """Finalization failed after tasks completed.

    Raised when:
    - Metadata parquet write fails
    - COLLECTION.json write fails
    - ZIP packaging fails (for folder2zip)
    - Permission denied on output directory

    Data files may exist but dataset is incomplete.
    """

    pass
