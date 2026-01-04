"""Integration tests for tacobridge.api module."""

import pytest
import tacoreader

from tacobridge import export, zip2folder, folder2zip


def _count(ds):
    """Get sample count from dataset."""
    return ds.pit_schema.root["n"]


def _to_df(ds):
    """Get level0 as pandas dataframe."""
    return ds._duckdb.execute("SELECT * FROM data").fetch_arrow_table().to_pandas()


class TestExport:

    def test_export_flat_to_folder(self, flat_a_zip, tmp_path):
        output = tmp_path / "out"
        result = export(flat_a_zip, output, output_format="folder")

        assert result.exists()
        assert (result / "COLLECTION.json").exists()
        assert (result / "METADATA" / "level0.parquet").exists()

    def test_export_flat_to_zip(self, flat_a_zip, tmp_path):
        output = tmp_path / "out.tacozip"
        result = export(flat_a_zip, output, output_format="zip")

        assert result.exists()
        assert result.suffix == ".tacozip"

    def test_export_auto_detects_zip(self, flat_a_zip, tmp_path):
        output = tmp_path / "out.tacozip"
        result = export(flat_a_zip, output)

        assert result.suffix == ".tacozip"

    def test_export_auto_detects_folder(self, flat_a_zip, tmp_path):
        output = tmp_path / "out"
        result = export(flat_a_zip, output)

        assert result.is_dir()

    def test_export_filtered(self, flat_a_zip, tmp_path):
        filtered = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 50")
        result = export(filtered, tmp_path / "out")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_export_nested(self, nested_a_zip, tmp_path):
        result = export(nested_a_zip, tmp_path / "out")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_export_nested_filtered(self, nested_a_zip, tmp_path):
        filtered = nested_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 30")
        result = export(filtered, tmp_path / "out")

        ds = tacoreader.load(result)
        assert _count(ds) == 2

    def test_export_deep(self, deep_zip, tmp_path):
        result = export(deep_zip, tmp_path / "out")

        ds = tacoreader.load(result)
        assert _count(ds) == 3

    def test_export_preserves_data(self, flat_a_zip, tmp_path):
        result = export(flat_a_zip, tmp_path / "out")

        ds = tacoreader.load(result)
        df = _to_df(ds)
        assert "cloud_cover" in df.columns
        assert "location" in df.columns

    def test_export_with_workers(self, flat_a_zip, tmp_path):
        result = export(flat_a_zip, tmp_path / "out", workers=4)

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_export_zip_strips_extension_for_folder(self, flat_a_zip, tmp_path):
        output = tmp_path / "out.tacozip"
        result = export(flat_a_zip, output, output_format="folder")

        assert result.name == "out"
        assert result.is_dir()


class TestZip2Folder:

    def test_converts_flat(self, flat_a_zip_path, tmp_path):
        result = zip2folder(flat_a_zip_path, tmp_path / "out")

        assert result.is_dir()
        assert (result / "COLLECTION.json").exists()
        assert (result / "DATA").is_dir()

    def test_converts_nested(self, nested_a_zip_path, tmp_path):
        result = zip2folder(nested_a_zip_path, tmp_path / "out")

        meta_files = list(result.rglob("__meta__"))
        assert len(meta_files) == 5

    def test_readable_after_conversion(self, flat_a_zip_path, tmp_path):
        result = zip2folder(flat_a_zip_path, tmp_path / "out")

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_data_integrity(self, flat_a_zip_path, tmp_path):
        result = zip2folder(flat_a_zip_path, tmp_path / "out")

        ds_original = tacoreader.load(flat_a_zip_path)
        ds_converted = tacoreader.load(result)

        df_orig = _to_df(ds_original)
        df_conv = _to_df(ds_converted)

        assert list(df_orig["id"]) == list(df_conv["id"])


class TestFolder2Zip:

    def test_converts_flat(self, flat_a_folder, tmp_path):
        result = folder2zip(flat_a_folder, tmp_path / "out.tacozip")

        assert result.exists()
        assert result.suffix == ".tacozip"

    def test_converts_nested(self, nested_a_folder, tmp_path):
        result = folder2zip(nested_a_folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 5

    def test_readable_after_conversion(self, flat_a_folder, tmp_path):
        result = folder2zip(flat_a_folder, tmp_path / "out.tacozip")

        ds = tacoreader.load(result)
        assert _count(ds) == 10

    def test_data_integrity(self, flat_a_folder, tmp_path):
        result = folder2zip(flat_a_folder, tmp_path / "out.tacozip")

        ds_original = tacoreader.load(flat_a_folder)
        ds_converted = tacoreader.load(result)

        df_orig = _to_df(ds_original)
        df_conv = _to_df(ds_converted)

        assert list(df_orig["id"]) == list(df_conv["id"])