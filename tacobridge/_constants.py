"""Constants for tacobridge operations.

Re-exports from tacoreader and tacotoolbox where appropriate.
Bridge-specific constants (column groups, templates) defined here.
"""

from tacoreader._constants import (
    COLUMN_ID,
    COLUMN_TYPE,
    METADATA_CURRENT_ID,
    METADATA_GDAL_VSI,
    METADATA_OFFSET,
    METADATA_PARENT_ID,
    METADATA_RELATIVE_PATH,
    METADATA_SIZE,
    METADATA_SOURCE_FILE,
    METADATA_SOURCE_PATH,
    PARQUET_EXTENSION,
    SAMPLE_TYPE_FILE,
    SAMPLE_TYPE_FOLDER,
    TACOZIP_EXTENSIONS,
)
from tacotoolbox._constants import (
    FOLDER_COLLECTION_FILENAME,
    FOLDER_DATA_DIR,
    FOLDER_META_FILENAME,
    FOLDER_METADATA_DIR,
)

__all__ = [
    # Re-exports: tacoreader
    "COLUMN_ID",
    "COLUMN_TYPE",
    "METADATA_CURRENT_ID",
    "METADATA_GDAL_VSI",
    "METADATA_OFFSET",
    "METADATA_PARENT_ID",
    "METADATA_RELATIVE_PATH",
    "METADATA_SIZE",
    "METADATA_SOURCE_FILE",
    "METADATA_SOURCE_PATH",
    "PARQUET_EXTENSION",
    "SAMPLE_TYPE_FILE",
    "SAMPLE_TYPE_FOLDER",
    "TACOZIP_EXTENSIONS",
    # Re-exports: tacotoolbox
    "FOLDER_COLLECTION_FILENAME",
    "FOLDER_DATA_DIR",
    "FOLDER_META_FILENAME",
    "FOLDER_METADATA_DIR",
    # Bridge-specific
    "ZIP_ONLY_COLUMNS",
    "CONCAT_COLUMNS",
    "EXPORT_STRIP_COLUMNS",
    "LEVEL_PARQUET_TEMPLATE",
    "TEMP_FOLDER_TEMPLATE",
    "PIT_SCHEMA_KEY",
    "FIELD_SCHEMA_KEY",
    "SUBSET_OF_KEY",
    "SUBSET_DATE_KEY",
    "VSI_SUBFILE_PREFIX",
]


ZIP_ONLY_COLUMNS: tuple[str, ...] = (
    METADATA_OFFSET,
    METADATA_SIZE,
)
"""Columns present only in ZIP/TacoCat formats, removed when converting to FOLDER."""

CONCAT_COLUMNS: tuple[str, ...] = (
    METADATA_SOURCE_PATH,
    METADATA_SOURCE_FILE,
)
"""Columns added during concat operations to track provenance."""

EXPORT_STRIP_COLUMNS: tuple[str, ...] = ZIP_ONLY_COLUMNS + CONCAT_COLUMNS
"""All columns to remove when exporting filtered/concatenated datasets."""

LEVEL_PARQUET_TEMPLATE = "level{}.parquet"
"""Template for consolidated metadata files."""

TEMP_FOLDER_TEMPLATE = ".{}_temp"
"""Template for temporary folders during ZIP export. Hidden with dot prefix."""

PIT_SCHEMA_KEY = "taco:pit_schema"
"""Key for Position-Invariant Tree schema in COLLECTION.json."""

FIELD_SCHEMA_KEY = "taco:field_schema"
"""Key for field descriptions schema in COLLECTION.json."""

SUBSET_OF_KEY = "taco:subset_of"
"""Key for tracking parent dataset when exporting filtered subsets."""

SUBSET_DATE_KEY = "taco:subset_date"
"""Key for timestamp when subset was created."""

VSI_SUBFILE_PREFIX = "/vsisubfile/"
"""GDAL VSI prefix for byte-range access within archives."""
