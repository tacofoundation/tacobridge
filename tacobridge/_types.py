"""Data types for tacobridge operations.

Three operation types with different semantics:

    ExportPlan / Zip2FolderPlan:
        - CopyTask: transfer bytes from src → dest (local or remote)
        - execute() performs actual I/O
        - finalize() writes metadata

    Folder2ZipPlan:
        - ZipEntry: reference to existing local file → arc path in ZIP
        - NO execute() needed (files already exist)
        - finalize() packages everything into ZIP
"""

from pathlib import Path

import pyarrow as pa
from pydantic import BaseModel

# --- Task Types ---


class CopyTask(BaseModel, frozen=True):
    """Single byte-transfer operation (for export/zip2folder).

    Represents copying bytes from a source (local file or remote URL)
    to a local destination path.

    Attributes:
        src: Source path (local filesystem) or URL (s3://, https://, etc.)
        dest: Destination path (local filesystem, absolute)
        offset: Byte offset for partial reads (ZIP entries), None for full file
        size: Byte count for partial reads (ZIP entries), None for full file
    """

    src: str
    dest: str
    offset: int | None = None
    size: int | None = None


class ZipEntry(BaseModel, frozen=True):
    """Reference to file for ZIP packaging (for folder2zip).

    NOT a copy operation - the file already exists locally.
    Used by ZipWriter to know what to package.

    Attributes:
        src: Absolute path to existing local file
        arc_path: Path inside the ZIP archive (e.g., "DATA/sample_0/image.tif")
    """

    src: str
    arc_path: str


# --- Plan Types ---


class ExportPlan(BaseModel, frozen=True, arbitrary_types_allowed=True):
    """Plan for export operation.

    Workflow: plan_export() → execute(task) for each → finalize(plan)

    Attributes:
        tasks: Byte-transfer operations to execute
        source: Original dataset path (for provenance)
        output: Output directory path
        levels: Reindexed metadata tables (level0, level1, ...)
        local_metadata: FOLDER __meta__ tables keyed by "DATA/{folder_path}/"
        collection: Updated COLLECTION.json dict
    """

    tasks: tuple[CopyTask, ...]
    source: str
    output: Path
    levels: tuple[pa.Table, ...] = ()
    local_metadata: dict[str, pa.Table] = {}
    collection: dict = {}


class Zip2FolderPlan(BaseModel, frozen=True, arbitrary_types_allowed=True):
    """Plan for ZIP to FOLDER conversion.

    Workflow: plan_zip2folder() → execute(task) for each → finalize(plan)

    Attributes:
        tasks: Byte-transfer operations to execute
        source: Source .tacozip path
        output: Output directory path
        levels: Metadata tables with ZIP columns stripped
        local_metadata: FOLDER __meta__ tables keyed by "DATA/{folder_path}/"
        collection: Original COLLECTION.json dict
    """

    tasks: tuple[CopyTask, ...]
    source: str
    output: Path
    levels: tuple[pa.Table, ...] = ()
    local_metadata: dict[str, pa.Table] = {}
    collection: dict = {}


class Folder2ZipPlan(BaseModel, frozen=True, arbitrary_types_allowed=True):
    """Plan for FOLDER to ZIP conversion.

    Workflow: plan_folder2zip() → finalize(plan)
    NOTE: No execute() step - files already exist locally.

    Attributes:
        entries: References to local files for ZIP packaging
        source: Source folder path
        output: Output .tacozip path
        levels: Metadata tables from METADATA/levelX.parquet
        local_metadata: Reconstructed __meta__ tables for ZipWriter
        collection: Original COLLECTION.json dict
    """

    entries: tuple[ZipEntry, ...]
    source: Path
    output: Path
    levels: tuple[pa.Table, ...] = ()
    local_metadata: dict[str, pa.Table] = {}
    collection: dict = {}
