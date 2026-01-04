"""Tests for tacobridge._metadata module."""

import pytest
import pyarrow as pa

from tacobridge._metadata import (
    strip_columns,
    reindex_table,
    build_local_metadata,
    get_source_key,
    prepare_collection,
)
from tacobridge._constants import (
    EXPORT_STRIP_COLUMNS,
    METADATA_CURRENT_ID,
    METADATA_PARENT_ID,
    METADATA_SOURCE_PATH,
    METADATA_SOURCE_FILE,
)


class TestStripColumns:

    def test_removes_specified_columns(self):
        table = pa.table({
            "id": ["a", "b"],
            "__offset__": [0, 100],
            "__size__": [50, 50],
            "value": [1, 2],
        })

        result = strip_columns(table, ("__offset__", "__size__"))

        assert "__offset__" not in result.schema.names
        assert "__size__" not in result.schema.names
        assert "id" in result.schema.names
        assert "value" in result.schema.names

    def test_ignores_missing_columns(self):
        table = pa.table({"id": ["a"], "value": [1]})

        result = strip_columns(table, ("__offset__", "__size__"))

        assert result.num_columns == 2

    def test_default_strips_export_columns(self):
        table = pa.table({
            "id": ["a"],
            "__offset__": [0],
            "__size__": [50],
            "__source_path__": ["/path"],
            "__source_file__": ["file.zip"],
        })

        result = strip_columns(table)

        for col in EXPORT_STRIP_COLUMNS:
            assert col not in result.schema.names


class TestReindexTable:

    def test_replaces_ids(self):
        table = pa.table({
            METADATA_CURRENT_ID: [10, 20, 30],
            METADATA_PARENT_ID: [10, 20, 30],
            "value": ["a", "b", "c"],
        })

        result = reindex_table(table, [0, 1, 2], [0, 1, 2])

        assert result.column(METADATA_CURRENT_ID).to_pylist() == [0, 1, 2]
        assert result.column(METADATA_PARENT_ID).to_pylist() == [0, 1, 2]

    def test_preserves_other_columns(self):
        table = pa.table({
            METADATA_CURRENT_ID: [5, 6],
            METADATA_PARENT_ID: [5, 6],
            "name": ["x", "y"],
            "score": [1.0, 2.0],
        })

        result = reindex_table(table, [0, 1], [0, 1])

        assert result.column("name").to_pylist() == ["x", "y"]
        assert result.column("score").to_pylist() == [1.0, 2.0]

    def test_different_parent_ids(self):
        table = pa.table({
            METADATA_CURRENT_ID: [100, 101, 102],
            METADATA_PARENT_ID: [50, 50, 51],
            "value": [1, 2, 3],
        })

        result = reindex_table(table, [0, 1, 2], [0, 0, 1])

        assert result.column(METADATA_CURRENT_ID).to_pylist() == [0, 1, 2]
        assert result.column(METADATA_PARENT_ID).to_pylist() == [0, 0, 1]


class TestBuildLocalMetadata:

    def test_empty_for_flat(self):
        table = pa.table({
            METADATA_CURRENT_ID: [0, 1, 2],
            METADATA_PARENT_ID: [0, 1, 2],
            "id": ["a", "b", "c"],
            "type": ["FILE", "FILE", "FILE"],
        })

        result = build_local_metadata([table])

        assert result == {}

    def test_creates_meta_for_folders(self):
        level0 = pa.table({
            METADATA_CURRENT_ID: [0, 1],
            METADATA_PARENT_ID: [0, 1],
            "id": ["folder_0", "folder_1"],
            "type": ["FOLDER", "FOLDER"],
        })
        level1 = pa.table({
            METADATA_CURRENT_ID: [0, 1, 2, 3],
            METADATA_PARENT_ID: [0, 0, 1, 1],
            "id": ["item_0", "item_1", "item_0", "item_1"],
            "type": ["FILE", "FILE", "FILE", "FILE"],
        })

        result = build_local_metadata([level0, level1])

        assert len(result) == 2
        assert "DATA/folder_0/" in result
        assert "DATA/folder_1/" in result
        assert result["DATA/folder_0/"].num_rows == 2
        assert result["DATA/folder_1/"].num_rows == 2


class TestGetSourceKey:

    def test_returns_source_path_if_present(self):
        row = {METADATA_SOURCE_PATH: "/path/to/file.zip", METADATA_SOURCE_FILE: "other"}
        key = get_source_key(row, has_source_path=True, has_source_file=True)
        assert key == "/path/to/file.zip"

    def test_returns_source_file_if_no_path(self):
        row = {METADATA_SOURCE_FILE: "data.tacozip"}
        key = get_source_key(row, has_source_path=False, has_source_file=True)
        assert key == "data.tacozip"

    def test_returns_empty_for_single_source(self):
        row = {"id": "sample_0"}
        key = get_source_key(row, has_source_path=False, has_source_file=False)
        assert key == ""


class TestPrepareCollection:

    def test_adds_subset_keys(self, flat_a_zip):
        collection = prepare_collection(flat_a_zip)

        assert "taco:subset_of" in collection
        assert "taco:subset_date" in collection

    def test_updates_root_count(self, flat_a_zip):
        filtered = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 30")
        collection = prepare_collection(filtered)

        assert collection["taco:pit_schema"]["root"]["n"] == 3