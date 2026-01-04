"""Execute single CopyTask. User controls parallelization.

This module only handles CopyTask (byte transfers).
Folder2ZipPlan uses ZipEntry which doesn't need execute() -
files already exist and are packaged directly by finalize().
"""

from pathlib import Path

from tacoreader._format import is_remote
from tacoreader._remote_io import download_bytes, download_range

from tacobridge._exceptions import TacoExecuteError
from tacobridge._types import CopyTask


def execute(task: CopyTask) -> None:
    """Execute single CopyTask (byte transfer).

    Reads bytes from source (local or remote) and writes to local destination.
    User controls parallelization by calling this in threads/processes.

    Args:
        task: CopyTask with src, dest, offset, size

    Raises:
        TacoExecuteError: If read or write fails

    Example:
        >>> from concurrent.futures import ThreadPoolExecutor
        >>> with ThreadPoolExecutor(8) as pool:
        ...     list(pool.map(execute, plan.tasks))
    """
    try:
        data = _read_bytes(task.src, task.offset, task.size)
        _write_bytes(task.dest, data)
    except TacoExecuteError:
        raise
    except Exception as e:
        raise TacoExecuteError(f"Failed: {task.src} -> {task.dest}: {e}") from e


def _read_bytes(src: str, offset: int | None, size: int | None) -> bytes:
    """Read bytes from local file or remote URL."""
    if is_remote(src):
        return _read_remote(src, offset, size)
    return _read_local(src, offset, size)


def _read_local(src: str, offset: int | None, size: int | None) -> bytes:
    """Read bytes from local filesystem."""
    with open(src, "rb") as f:
        if offset is not None:
            f.seek(offset)
        if size is not None:
            return f.read(size)
        return f.read()


def _read_remote(src: str, offset: int | None, size: int | None) -> bytes:
    """Read bytes from remote URL using tacoreader's I/O."""
    if offset is not None and size is not None:
        return bytes(download_range(src, offset, size))
    return bytes(download_bytes(src))


def _write_bytes(dest: str, data: bytes) -> None:
    """Write bytes to local destination, creating parent dirs."""
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(data)
