"""Metadata manipulation for tacobridge operations.

Handles reindexing, stripping format-specific columns, and building
local metadata structures for export operations.
"""

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

from tacobridge._constants import (
    COLUMN_TYPE,
    EXPORT_STRIP_COLUMNS,
    FOLDER_DATA_DIR,
    METADATA_CURRENT_ID,
    METADATA_PARENT_ID,
    METADATA_RELATIVE_PATH,
    METADATA_SOURCE_FILE,
    METADATA_SOURCE_PATH,
    PIT_SCHEMA_KEY,
    SAMPLE_TYPE_FOLDER,
    SUBSET_DATE_KEY,
    SUBSET_OF_KEY,
)

if TYPE_CHECKING:
    from tacoreader import TacoDataset


def get_source_key(row: dict[str, Any], has_source_path: bool, has_source_file: bool) -> str:
    """Extract source key from row for composite keying in concat datasets.

    Returns empty string for single-source datasets.
    """
    if has_source_path and row.get(METADATA_SOURCE_PATH):
        return str(row[METADATA_SOURCE_PATH])
    if has_source_file and row.get(METADATA_SOURCE_FILE):
        return str(row[METADATA_SOURCE_FILE])
    return ""


def strip_columns(table: pa.Table, columns: tuple[str, ...] | None = None) -> pa.Table:
    """Remove specified columns from table if present.

    Args:
        table: PyArrow table
        columns: Columns to remove. Defaults to EXPORT_STRIP_COLUMNS.

    Returns:
        Table with columns removed.
    """
    if columns is None:
        columns = EXPORT_STRIP_COLUMNS

    for col in columns:
        if col in table.schema.names:
            table = table.drop([col])
    return table


def strip_zip_columns(dataset: "TacoDataset") -> list[pa.Table]:
    """Get all levels from dataset with format-specific columns removed."""
    levels: list[pa.Table] = []
    max_depth: int = dataset.pit_schema.max_depth()

    for level_idx in range(max_depth + 1):
        view_name = f"level{level_idx}"
        table: pa.Table = dataset._duckdb.execute(f"SELECT * FROM {view_name}").fetch_arrow_table()
        table = strip_columns(table)
        levels.append(table)

    return levels


def reindex_table(
    table: pa.Table,
    new_current_ids: list[int] | range,
    new_parent_ids: list[int] | range,
) -> pa.Table:
    """Replace current_id and parent_id columns with new sequential values."""
    current_idx = table.schema.get_field_index(METADATA_CURRENT_ID)
    parent_idx = table.schema.get_field_index(METADATA_PARENT_ID)

    table = table.set_column(
        current_idx,
        METADATA_CURRENT_ID,
        pa.array(list(new_current_ids), type=pa.int64()),
    )
    table = table.set_column(
        parent_idx,
        METADATA_PARENT_ID,
        pa.array(list(new_parent_ids), type=pa.int64()),
    )
    return table


def reindex_metadata_from_snapshot(
    dataset: "TacoDataset",
    level0_snapshot: pa.Table,
) -> tuple[list[pa.Table], dict[str, pa.Table]]:
    """Reindex metadata using pre-fetched level0 snapshot.

    For concatenated datasets, uses (source_key, current_id) as composite key
    since current_id values can repeat across different sources.

    Uses _filtered_level_views when available (from cascade filter operations)
    to ensure exported metadata matches the filtered dataset structure.

    Args:
        dataset: Source TacoDataset for accessing deeper levels
        level0_snapshot: Pre-fetched level0 table (avoids RANDOM() re-evaluation)

    Returns:
        Tuple of (levels list, local_metadata dict)
    """
    levels: list[pa.Table] = []
    parent_id_mapping: dict[tuple[str, int], int] = {}
    max_depth: int = dataset.pit_schema.max_depth()

    has_source_path = METADATA_SOURCE_PATH in level0_snapshot.schema.names
    has_source_file = METADATA_SOURCE_FILE in level0_snapshot.schema.names

    filtered_level_views: dict[int, str] = getattr(dataset, "_filtered_level_views", {})

    for level_idx in range(max_depth + 1):
        if level_idx == 0:
            rows: list[dict[str, Any]] = level0_snapshot.to_pylist()

            for new_idx, row in enumerate(rows):
                source_key = get_source_key(row, has_source_path, has_source_file)
                old_id = int(row[METADATA_CURRENT_ID])
                parent_id_mapping[(source_key, old_id)] = new_idx

            table = level0_snapshot
            table = reindex_table(table, range(table.num_rows), range(table.num_rows))
        else:
            view_name = filtered_level_views.get(level_idx, f"level{level_idx}")
            table = dataset._duckdb.execute(f"SELECT * FROM {view_name}").fetch_arrow_table()
            rows = table.to_pylist()

            filtered_rows: list[dict[str, Any]] = []
            new_parent_ids: list[int] = []

            for row in rows:
                source_key = get_source_key(row, has_source_path, has_source_file)
                old_parent_id = int(row[METADATA_PARENT_ID])
                key = (source_key, old_parent_id)

                if key in parent_id_mapping:
                    filtered_rows.append(row)
                    new_parent_ids.append(parent_id_mapping[key])

            for new_idx, row in enumerate(filtered_rows):
                source_key = get_source_key(row, has_source_path, has_source_file)
                old_id = int(row[METADATA_CURRENT_ID])
                parent_id_mapping[(source_key, old_id)] = new_idx

            if filtered_rows:
                table = pa.Table.from_pylist(filtered_rows, schema=table.schema)
            else:
                table = table.slice(0, 0)

            table = reindex_table(table, range(table.num_rows), new_parent_ids)

        table = strip_columns(table)
        levels.append(table)

    local_metadata = build_local_metadata(levels)
    return levels, local_metadata


def build_local_metadata(levels: list[pa.Table]) -> dict[str, pa.Table]:
    """Build local __meta__ tables from consolidated levels.

    Creates a mapping from folder paths to their children's metadata tables.
    Used for writing DATA/{folder}/__meta__ files.

    Builds paths recursively by tracking parent paths per level, ensuring correct
    full paths for nested folder structures (e.g., DATA/date/region/).

    Args:
        levels: List of consolidated level tables

    Returns:
        Dict mapping "DATA/{folder_path}/" to children PyArrow tables
    """
    local_metadata: dict[str, pa.Table] = {}
    paths_by_level: list[dict[int, str]] = [{} for _ in range(len(levels))]

    for level_idx in range(len(levels) - 1):
        current_level = levels[level_idx]
        next_level = levels[level_idx + 1]

        next_level_rows: list[dict[str, Any]] = next_level.to_pylist()
        next_parent_ids = [int(r[METADATA_PARENT_ID]) for r in next_level_rows]

        folders_mask = pc.equal(current_level.column(COLUMN_TYPE), pa.scalar(SAMPLE_TYPE_FOLDER))
        folders: pa.Table = current_level.filter(folders_mask)

        for folder in folders.to_pylist():
            current_id = int(folder[METADATA_CURRENT_ID])
            parent_id = int(folder[METADATA_PARENT_ID])

            if level_idx == 0:
                rel_path = folder["id"]
            else:
                parent_path = paths_by_level[level_idx - 1].get(parent_id, "")
                rel_path = f"{parent_path}/{folder['id']}" if parent_path else folder["id"]

            paths_by_level[level_idx][current_id] = rel_path
            folder_path = f"{FOLDER_DATA_DIR}/{rel_path}/"

            children_indices = [i for i, pid in enumerate(next_parent_ids) if pid == current_id]

            if children_indices:
                children: pa.Table = next_level.take(children_indices)
            else:
                children = next_level.slice(0, 0)

            if METADATA_RELATIVE_PATH in children.schema.names:
                children = children.drop([METADATA_RELATIVE_PATH])

            local_metadata[folder_path] = children

    return local_metadata


def prepare_collection(dataset: "TacoDataset") -> dict[str, Any]:
    """Prepare COLLECTION.json with updated counts and subset provenance.

    Marks the exported dataset as a subset of the original and records
    the export timestamp.
    """
    collection: dict[str, Any] = dataset.collection.copy()
    collection[PIT_SCHEMA_KEY]["root"]["n"] = dataset.pit_schema.root["n"]
    collection[SUBSET_OF_KEY] = collection.get("id", "unknown")
    collection[SUBSET_DATE_KEY] = datetime.now(UTC).isoformat()
    return collection
