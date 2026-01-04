"""Format bridge for AI-ready datasets.

tacobridge connects tacotoolbox (creation) and tacoreader (querying),
providing format conversion and filtered export capabilities.
"""

from tacobridge.api import export, folder2zip, zip2folder
from tacobridge.execute import execute
from tacobridge.finalize import finalize
from tacobridge.plan import plan_export, plan_folder2zip, plan_zip2folder


def _get_version() -> str:
    """Get package version."""
    try:
        from importlib import metadata

        return metadata.version("tacobridge")
    except (ImportError, ModuleNotFoundError):
        return "0.0.0"


__version__ = _get_version()

__all__ = [
    # High-level API
    "export",
    "folder2zip",
    "zip2folder",
    # Low-level API (plan/execute/finalize)
    "plan_export",
    "plan_folder2zip",
    "plan_zip2folder",
    "execute",
    "finalize",
]
