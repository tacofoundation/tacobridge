"""Finalize operations after tasks are executed.

Two finalization paths:

    ExportPlan / Zip2FolderPlan:
        - Write METADATA/levelX.parquet (consolidated)
        - Write DATA/*/__meta__ (local metadata)
        - Write COLLECTION.json

    Folder2ZipPlan:
        - Package files into ZIP using ZipWriter
"""

import json
from pathlib import Path
from typing import Any

from tacotoolbox._column_utils import write_parquet_file, write_parquet_file_with_cdc
from tacotoolbox._metadata import MetadataPackage
from tacotoolbox._writers.zip_writer import ZipWriter

from tacobridge._constants import (
    FIELD_SCHEMA_KEY,
    FOLDER_COLLECTION_FILENAME,
    FOLDER_META_FILENAME,
    FOLDER_METADATA_DIR,
    LEVEL_PARQUET_TEMPLATE,
    PIT_SCHEMA_KEY,
)
from tacobridge._exceptions import TacoFinalizeError
from tacobridge._logging import get_logger
from tacobridge._types import ExportPlan, Folder2ZipPlan, Zip2FolderPlan

logger = get_logger(__name__)


def finalize(plan: ExportPlan | Zip2FolderPlan | Folder2ZipPlan) -> Path:
    """Finalize operation by writing metadata or packaging ZIP.

    Args:
        plan: Completed plan (tasks already executed for Export/Zip2Folder)

    Returns:
        Path to output

    Raises:
        TacoFinalizeError: If metadata write or ZIP creation fails
    """
    try:
        if isinstance(plan, Folder2ZipPlan):
            return _finalize_folder2zip(plan)
        return _finalize_to_folder(plan)
    except TacoFinalizeError:
        raise
    except Exception as e:
        raise TacoFinalizeError(f"Failed to finalize: {e}") from e


def _finalize_to_folder(plan: ExportPlan | Zip2FolderPlan) -> Path:
    """Write FOLDER structure: METADATA/, DATA/__meta__, COLLECTION.json."""
    output = plan.output
    metadata_dir = output / FOLDER_METADATA_DIR
    metadata_dir.mkdir(parents=True, exist_ok=True)

    for i, table in enumerate(plan.levels):
        path = metadata_dir / LEVEL_PARQUET_TEMPLATE.format(i)
        write_parquet_file_with_cdc(table, path)
        logger.debug(f"Wrote {path.name}: {table.num_rows} rows")

    for folder_path, table in plan.local_metadata.items():
        meta_path = output / folder_path / FOLDER_META_FILENAME
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_file(table, meta_path)
        logger.debug(f"Wrote {meta_path}")

    collection_path = output / FOLDER_COLLECTION_FILENAME
    collection_path.write_text(
        json.dumps(plan.collection, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.debug(f"Wrote {FOLDER_COLLECTION_FILENAME}")

    logger.info(f"Finalized FOLDER: {output}")
    return output


def _finalize_folder2zip(plan: Folder2ZipPlan) -> Path:
    """Package FOLDER into ZIP using ZipWriter."""
    src_files = [entry.src for entry in plan.entries]
    arc_files = [entry.arc_path for entry in plan.entries]

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

    writer = ZipWriter(output_path=plan.output)
    result: Path = writer.create_complete_zip(
        src_files=src_files,
        arc_files=arc_files,
        metadata_package=metadata_package,
    )

    logger.info(f"Finalized ZIP: {result}")
    return result
