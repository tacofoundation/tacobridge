"""High-level convenience API for tacobridge.

Three operations:
    export: Extract TacoDataset to FOLDER or ZIP format
    zip2folder: Convert ZIP to FOLDER format
    folder2zip: Convert FOLDER to ZIP format

Format auto-detection (like tacotoolbox.create):
    - .zip/.tacozip -> ZIP format
    - anything else -> FOLDER format
"""

import json
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import tacoreader
from tacotoolbox._column_utils import write_parquet_file, write_parquet_file_with_cdc
from tacotoolbox._metadata import MetadataPackage
from tacotoolbox._writers.zip_writer import ZipWriter
from tqdm import tqdm

from tacobridge._constants import (
    FIELD_SCHEMA_KEY,
    FOLDER_COLLECTION_FILENAME,
    FOLDER_DATA_DIR,
    FOLDER_META_FILENAME,
    FOLDER_METADATA_DIR,
    PIT_SCHEMA_KEY,
    TACOZIP_EXTENSIONS,
    TEMP_FOLDER_TEMPLATE,
)
from tacobridge._logging import get_logger
from tacobridge._metadata import build_local_metadata, strip_zip_columns
from tacobridge._types import CopyTask, ExportPlan
from tacobridge.execute import execute
from tacobridge.finalize import finalize
from tacobridge.plan import plan_export, plan_folder2zip, plan_zip2folder

if TYPE_CHECKING:
    from tacoreader import TacoDataset

logger = get_logger(__name__)


def export(
    dataset: "TacoDataset",
    output: str | Path,
    output_format: Literal["zip", "folder", "auto"] = "auto",
    workers: int = 1,
    progress: bool = True,
    temp_dir: str | Path | None = None,
) -> Path:
    """Export TacoDataset to FOLDER or ZIP format.

    Dataset can be filtered, concatenated, or transformed before export.

    Args:
        dataset: TacoDataset instance (can be filtered, concatenated, etc.)
        output: Path to output (file for ZIP, directory for FOLDER)
        output_format: Container format ("zip", "folder", or "auto")
        workers: Number of parallel workers (1 = sequential, >1 = threaded)
        progress: Show progress bar
        temp_dir: Directory for temporary files when creating ZIP

    Returns:
        Path to created output

    Example:
        >>> ds = tacoreader.load("big.tacozip")
        >>> ds = ds.sql("SELECT * FROM data WHERE country = 'ES'")
        >>> tacobridge.export(ds, "spain.tacozip")
    """
    output = Path(output)
    final_format = _detect_format(output) if output_format == "auto" else output_format

    if final_format == "folder" and output.suffix.lower() in TACOZIP_EXTENSIONS:
        output = output.with_suffix("")

    if final_format == "folder":
        plan = plan_export(dataset, output)
        _execute_tasks(plan.tasks, workers=workers, progress=progress, desc="Exporting")
        return finalize(plan)

    plan = plan_export(dataset, output.with_suffix(""))
    return _export_to_zip(plan, output, workers, progress, temp_dir)


def zip2folder(
    source: str | Path,
    output: str | Path,
    workers: int = 1,
    progress: bool = True,
) -> Path:
    """Convert ZIP to FOLDER format.

    Args:
        source: Path to source .tacozip
        output: Path to output directory
        workers: Number of parallel workers (1 = sequential, >1 = threaded)
        progress: Show progress bar

    Returns:
        Path to created folder
    """
    from tacoreader._format import is_remote

    source = Path(source)
    output = Path(output)

    if not is_remote(str(source)):
        return _zip2folder_local(source, output, progress)

    plan = plan_zip2folder(str(source), output)
    _execute_tasks(plan.tasks, workers=workers, progress=progress, desc="Extracting")
    return finalize(plan)


def folder2zip(
    source: str | Path,
    output: str | Path,
    progress: bool = True,
) -> Path:
    """Convert FOLDER to ZIP format.

    Args:
        source: Path to source folder
        output: Path to output .tacozip
        progress: Show progress bar

    Returns:
        Path to created ZIP
    """
    plan = plan_folder2zip(source, output)

    if progress:
        logger.info(f"Packaging {len(plan.entries)} files into ZIP...")

    return finalize(plan)


def _detect_format(output: Path) -> Literal["zip", "folder"]:
    """Auto-detect format from output path extension."""
    if output.suffix.lower() in TACOZIP_EXTENSIONS:
        return "zip"
    return "folder"


def _export_to_zip(
    plan: ExportPlan,
    output: Path,
    workers: int,
    progress: bool,
    temp_dir: str | Path | None,
) -> Path:
    """Export to ZIP using temporary folder for data files."""
    temp_folder = _create_temp_path(output, temp_dir)

    try:
        temp_plan = _relocate_plan(plan, temp_folder)
        _execute_tasks(temp_plan.tasks, workers=workers, progress=progress, desc="Exporting")
        _write_local_metadata(plan.local_metadata, temp_folder)
        return _package_to_zip(temp_folder, output, plan, progress)
    finally:
        if temp_folder.exists():
            shutil.rmtree(temp_folder, ignore_errors=True)


def _create_temp_path(output: Path, temp_dir: str | Path | None) -> Path:
    """Create temporary folder path for ZIP export."""
    if temp_dir is None:
        return output.parent / TEMP_FOLDER_TEMPLATE.format(output.stem)
    return Path(temp_dir) / TEMP_FOLDER_TEMPLATE.format(output.stem)


def _relocate_plan(plan: ExportPlan, temp_folder: Path) -> ExportPlan:
    """Create new plan with tasks redirected to temp folder."""
    return ExportPlan(
        tasks=tuple(
            task.model_copy(update={"dest": str(temp_folder / Path(task.dest).relative_to(plan.output))})
            for task in plan.tasks
        ),
        source=plan.source,
        output=temp_folder,
        levels=plan.levels,
        local_metadata=plan.local_metadata,
        collection=plan.collection,
    )


def _write_local_metadata(local_metadata: dict[str, Any], temp_folder: Path) -> None:
    """Write __meta__ files to temp folder."""
    for folder_path, table in local_metadata.items():
        meta_path = temp_folder / folder_path / FOLDER_META_FILENAME
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_file(table, meta_path)


def _package_to_zip(temp_folder: Path, output: Path, plan: ExportPlan, progress: bool) -> Path:
    """Package temp folder contents into ZIP."""
    data_dir = temp_folder / FOLDER_DATA_DIR
    src_files: list[str] = []
    arc_files: list[str] = []

    for file_path in data_dir.rglob("*"):
        if file_path.is_file() and file_path.name != FOLDER_META_FILENAME:
            rel = file_path.relative_to(data_dir)
            src_files.append(str(file_path))
            arc_files.append(f"{FOLDER_DATA_DIR}/{rel}")

    pit_schema: dict[str, Any] = plan.collection[PIT_SCHEMA_KEY]
    field_schema: dict[str, Any] = plan.collection[FIELD_SCHEMA_KEY]

    metadata_package = MetadataPackage(
        levels=list(plan.levels),
        local_metadata=plan.local_metadata,
        collection=plan.collection,
        pit_schema=pit_schema,
        field_schema=field_schema,
        max_depth=len(plan.levels) - 1,
    )

    if progress:
        logger.info(f"Packaging {len(src_files)} files into ZIP...")

    writer = ZipWriter(output_path=output)
    result: Path = writer.create_complete_zip(
        src_files=src_files,
        arc_files=arc_files,
        metadata_package=metadata_package,
    )
    return result


def _zip2folder_local(source: Path, output: Path, progress: bool) -> Path:
    """Fast extraction for local ZIPs using zipfile."""
    dataset: TacoDataset = tacoreader.load(source)
    levels = strip_zip_columns(dataset)
    local_metadata = build_local_metadata(levels)
    collection: dict[str, Any] = dataset.collection.copy()

    output.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(source, "r") as zf:
        data_members = [
            m for m in zf.namelist() if m.startswith(f"{FOLDER_DATA_DIR}/") and not m.endswith(FOLDER_META_FILENAME)
        ]

        members_iter = tqdm(data_members, desc="Extracting", unit="file") if progress else data_members
        for member in members_iter:
            zf.extract(member, output)

    metadata_dir = output / FOLDER_METADATA_DIR
    metadata_dir.mkdir(parents=True, exist_ok=True)

    for i, table in enumerate(levels):
        write_parquet_file_with_cdc(table, metadata_dir / f"level{i}.parquet")

    for folder_path, table in local_metadata.items():
        meta_path = output / folder_path / FOLDER_META_FILENAME
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_file(table, meta_path)

    collection_path = output / FOLDER_COLLECTION_FILENAME
    collection_path.write_text(
        json.dumps(collection, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info(f"Extracted to FOLDER: {output}")
    return output


def _execute_tasks(tasks: tuple[CopyTask, ...], workers: int, progress: bool, desc: str) -> None:
    """Execute tasks with optional parallelization."""
    if not tasks:
        return

    if workers <= 1:
        task_iter = tqdm(tasks, desc=desc, unit="file") if progress else tasks
        for task in task_iter:
            execute(task)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(execute, task) for task in tasks]
            future_iter = (
                tqdm(as_completed(futures), desc=desc, unit="file", total=len(futures))
                if progress
                else as_completed(futures)
            )
            for future in future_iter:
                future.result()
