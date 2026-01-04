"""Planning functions for tacobridge operations.

Plans compute what needs to be done without executing anything.
User controls execution strategy (sequential, threads, processes).
"""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import tacoreader
from tacoreader._constants import VSI_SUBFILE_PREFIX
from tacoreader._vsi import parse_vsi_subfile, strip_vsi_prefix

from tacobridge._constants import (
    COLUMN_ID,
    FOLDER_COLLECTION_FILENAME,
    FOLDER_DATA_DIR,
    FOLDER_META_FILENAME,
    FOLDER_METADATA_DIR,
    METADATA_CURRENT_ID,
    METADATA_GDAL_VSI,
    METADATA_PARENT_ID,
    METADATA_RELATIVE_PATH,
    METADATA_SOURCE_FILE,
    METADATA_SOURCE_PATH,
    SAMPLE_TYPE_FOLDER,
)
from tacobridge._exceptions import TacoPlanError
from tacobridge._logging import get_logger
from tacobridge._metadata import (
    build_local_metadata,
    prepare_collection,
    reindex_metadata_from_snapshot,
    strip_zip_columns,
)
from tacobridge._types import CopyTask, ExportPlan, Folder2ZipPlan, Zip2FolderPlan, ZipEntry

if TYPE_CHECKING:
    from tacoreader import TacoDataset

logger = get_logger(__name__)


def plan_export(dataset: "TacoDataset", output: str | Path) -> ExportPlan:
    """Plan export operation from TacoDataset."""
    output = Path(output)

    if output.exists():
        raise TacoPlanError(f"Output already exists: {output}")

    if dataset.pit_schema.root["n"] == 0:
        raise TacoPlanError("Dataset is empty")

    if dataset._has_level1_joins:
        raise TacoPlanError("Cannot export dataset with level1+ JOINs")

    # CRITICAL: Read filtered level0 ONCE to avoid RANDOM() re-evaluation
    level0_snapshot: pa.Table = dataset._duckdb.execute(f"SELECT * FROM {dataset._view_name}").fetch_arrow_table()

    tasks = _collect_copy_tasks_from_snapshot(dataset, level0_snapshot, output)
    levels, local_metadata = reindex_metadata_from_snapshot(dataset, level0_snapshot)
    collection = prepare_collection(dataset)

    return ExportPlan(
        tasks=tuple(tasks),
        source=dataset._path,
        output=output,
        levels=tuple(levels),
        local_metadata=local_metadata,
        collection=collection,
    )


def plan_zip2folder(source: str | Path, output: str | Path) -> Zip2FolderPlan:
    """Plan ZIP to FOLDER conversion."""
    source = str(source)
    output = Path(output)

    if output.exists():
        raise TacoPlanError(f"Output already exists: {output}")

    try:
        dataset: TacoDataset = tacoreader.load(source)
    except Exception as e:
        raise TacoPlanError(f"Failed to open dataset: {source}: {e}") from e

    tasks = _collect_copy_tasks(dataset, output)
    levels = strip_zip_columns(dataset)
    local_metadata = build_local_metadata(levels)
    collection: dict[str, Any] = dataset.collection.copy()

    return Zip2FolderPlan(
        tasks=tuple(tasks),
        source=source,
        output=output,
        levels=tuple(levels),
        local_metadata=local_metadata,
        collection=collection,
    )


def plan_folder2zip(source: str | Path, output: str | Path) -> Folder2ZipPlan:
    """Plan FOLDER to ZIP conversion."""
    source = Path(source)
    output = Path(output)

    if output.exists():
        raise TacoPlanError(f"Output already exists: {output}")

    if not source.exists():
        raise TacoPlanError(f"Source folder not found: {source}")

    collection = _read_collection(source)
    levels = _read_consolidated_metadata(source)
    local_metadata = build_local_metadata(levels)
    entries = _scan_folder_files(source)

    return Folder2ZipPlan(
        entries=tuple(entries),
        source=source,
        output=output,
        levels=tuple(levels),
        local_metadata=local_metadata,
        collection=collection,
    )


def _collect_copy_tasks(dataset: "TacoDataset", output: Path) -> list[CopyTask]:
    """Collect all CopyTasks from dataset for byte transfer."""
    table: pa.Table = dataset._duckdb.execute(f"SELECT * FROM {dataset._view_name}").fetch_arrow_table()
    return _collect_copy_tasks_from_snapshot(dataset, table, output)


def _collect_copy_tasks_from_snapshot(
    dataset: "TacoDataset",
    level0_snapshot: pa.Table,
    output: Path,
) -> list[CopyTask]:
    """Collect all CopyTasks from snapshot for byte transfer."""
    tasks: list[CopyTask] = []
    data_dir = output / FOLDER_DATA_DIR

    for row in level0_snapshot.to_pylist():
        if row["type"] == SAMPLE_TYPE_FOLDER:
            children_tasks = _collect_folder_children(dataset, row, data_dir, level=0)
            tasks.extend(children_tasks)
        else:
            vsi_path = row[METADATA_GDAL_VSI]
            rel_path = row.get(METADATA_RELATIVE_PATH) or row[COLUMN_ID]
            dest = data_dir / rel_path
            task = _vsi_to_copy_task(vsi_path, str(dest))
            tasks.append(task)

    return tasks


def _collect_folder_children(
    dataset: "TacoDataset",
    folder_row: dict[str, Any],
    data_dir: Path,
    level: int,
) -> list[CopyTask]:
    """Recursively collect CopyTasks for folder children."""
    tasks: list[CopyTask] = []
    current_id = folder_row[METADATA_CURRENT_ID]
    next_level = level + 1
    view_name = f"level{next_level}"

    max_depth: int = dataset.pit_schema.max_depth()
    if next_level > max_depth:
        return tasks

    children = _query_children(
        dataset,
        view_name,
        current_id,
        folder_row.get(METADATA_SOURCE_PATH),
        folder_row.get(METADATA_SOURCE_FILE),
    )

    for child in children.to_pylist():
        if child["type"] == SAMPLE_TYPE_FOLDER:
            sub_tasks = _collect_folder_children(dataset, child, data_dir, next_level)
            tasks.extend(sub_tasks)
        else:
            vsi_path = child[METADATA_GDAL_VSI]
            rel_path = child.get(METADATA_RELATIVE_PATH) or child[COLUMN_ID]
            dest = data_dir / rel_path
            task = _vsi_to_copy_task(vsi_path, str(dest))
            tasks.append(task)

    return tasks


def _query_children(
    dataset: "TacoDataset",
    view_name: str,
    parent_id: int,
    source_path: str | None,
    source_file: str | None,
) -> pa.Table:
    """Query children from dataset with appropriate source filtering."""
    if source_path:
        table: pa.Table = dataset._duckdb.execute(
            f'SELECT * FROM {view_name} WHERE "{METADATA_PARENT_ID}" = ? AND "{METADATA_SOURCE_PATH}" = ?',
            [parent_id, source_path],
        ).fetch_arrow_table()
        return table

    if source_file:
        table = dataset._duckdb.execute(
            f'SELECT * FROM {view_name} WHERE "{METADATA_PARENT_ID}" = ? AND "{METADATA_SOURCE_FILE}" = ?',
            [parent_id, source_file],
        ).fetch_arrow_table()
        return table

    table = dataset._duckdb.execute(
        f'SELECT * FROM {view_name} WHERE "{METADATA_PARENT_ID}" = ?',
        [parent_id],
    ).fetch_arrow_table()
    return table


def _vsi_to_copy_task(vsi_path: str, dest: str) -> CopyTask:
    """Convert GDAL VSI path to CopyTask."""
    if vsi_path.startswith(VSI_SUBFILE_PREFIX):
        zip_path, offset, size = parse_vsi_subfile(vsi_path)
        src = strip_vsi_prefix(zip_path)
        return CopyTask(src=src, dest=dest, offset=offset, size=size)
    return CopyTask(src=vsi_path, dest=dest, offset=None, size=None)


def _read_collection(folder: Path) -> dict[str, Any]:
    """Read and validate COLLECTION.json from folder."""
    path = folder / FOLDER_COLLECTION_FILENAME
    if not path.exists():
        raise TacoPlanError(f"{FOLDER_COLLECTION_FILENAME} not found: {path}")

    try:
        result: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return result
    except json.JSONDecodeError as e:
        raise TacoPlanError(f"Invalid {FOLDER_COLLECTION_FILENAME}: {e}") from e


def _read_consolidated_metadata(folder: Path) -> list[pa.Table]:
    """Read METADATA/levelX.parquet files in order."""
    metadata_dir = folder / FOLDER_METADATA_DIR
    if not metadata_dir.exists():
        raise TacoPlanError(f"{FOLDER_METADATA_DIR} directory not found: {metadata_dir}")

    level_files = sorted(metadata_dir.glob("level*.parquet"))
    if not level_files:
        raise TacoPlanError(f"No level*.parquet files found: {metadata_dir}")

    return [pq.read_table(f) for f in level_files]


def _scan_folder_files(folder: Path) -> list[ZipEntry]:
    """Scan DATA/ for files to include in ZIP."""
    data_dir = folder / FOLDER_DATA_DIR
    if not data_dir.exists():
        raise TacoPlanError(f"{FOLDER_DATA_DIR} directory not found: {data_dir}")

    entries: list[ZipEntry] = []
    for file_path in data_dir.rglob("*"):
        if file_path.is_file() and file_path.name != FOLDER_META_FILENAME:
            rel = file_path.relative_to(data_dir)
            arc_path = f"{FOLDER_DATA_DIR}/{rel}"
            entries.append(ZipEntry(src=str(file_path), arc_path=arc_path))

    if not entries:
        raise TacoPlanError(f"No data files found: {data_dir}")

    return entries
