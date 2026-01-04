"""Integration tests for roundtrip conversions."""

import pytest
import tacoreader

from tacobridge import zip2folder, folder2zip, export


def _count(ds):
    return ds.pit_schema.root["n"]


def _to_df(ds):
    return ds._duckdb.execute("SELECT * FROM data").fetch_arrow_table().to_pandas()


class TestZipFolderZip:

    def test_flat_roundtrip(self, flat_a_zip_path, tmp_path):
        folder = zip2folder(flat_a_zip_path, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_nested_roundtrip(self, nested_a_zip_path, tmp_path):
        folder = zip2folder(nested_a_zip_path, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_data_integrity(self, flat_a_zip_path, tmp_path):
        ds_original = tacoreader.load(flat_a_zip_path)
        df_orig = _to_df(ds_original)

        folder = zip2folder(flat_a_zip_path, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds_final = tacoreader.load(result)
        df_final = _to_df(ds_final)

        assert list(df_orig["id"]) == list(df_final["id"])
        assert list(df_orig["location"]) == list(df_final["location"])

    def test_nested_children_intact(self, nested_a_zip_path, tmp_path):
        folder = zip2folder(nested_a_zip_path, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        children = ds.data.read(0)  # First folder's children
        assert children.shape[0] == 3


class TestFolderZipFolder:

    def test_flat_roundtrip(self, flat_a_folder, tmp_path):
        zip_path = folder2zip(flat_a_folder, tmp_path / "out.tacozip")
        result = zip2folder(zip_path, tmp_path / "folder")

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_nested_roundtrip(self, nested_a_folder, tmp_path):
        zip_path = folder2zip(nested_a_folder, tmp_path / "out.tacozip")
        result = zip2folder(zip_path, tmp_path / "folder")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_data_integrity(self, flat_a_folder, tmp_path):
        ds_original = tacoreader.load(flat_a_folder)
        df_orig = _to_df(ds_original)

        zip_path = folder2zip(flat_a_folder, tmp_path / "out.tacozip")
        result = zip2folder(zip_path, tmp_path / "folder")

        ds_final = tacoreader.load(result)
        df_final = _to_df(ds_final)

        assert list(df_orig["id"]) == list(df_final["id"])


class TestExportRoundtrip:

    def test_export_folder_then_zip(self, flat_a_zip, tmp_path):
        folder = export(flat_a_zip, tmp_path / "folder", output_format="folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_filtered_roundtrip(self, flat_a_zip, tmp_path):
        filtered = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 50")
        folder = export(filtered, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_nested_filtered_roundtrip(self, nested_a_zip, tmp_path):
        filtered = nested_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 30")
        folder = export(filtered, tmp_path / "folder")
        result = folder2zip(folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 2