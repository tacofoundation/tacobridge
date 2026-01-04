"""Tests for tacobridge.finalize module."""

import json
import pytest

import pyarrow.parquet as pq

from tacobridge.plan import plan_export, plan_zip2folder, plan_folder2zip
from tacobridge.execute import execute
from tacobridge.finalize import finalize
from tacobridge._exceptions import TacoFinalizeError


class TestFinalizeExport:

    def test_creates_output_dir(self, flat_a_zip, tmp_path):
        output = tmp_path / "out"
        plan = plan_export(flat_a_zip, output)
        for task in plan.tasks:
            execute(task)

        result = finalize(plan)

        assert result.exists()
        assert result.is_dir()

    def test_writes_collection_json(self, flat_a_zip, tmp_path):
        output = tmp_path / "out"
        plan = plan_export(flat_a_zip, output)
        for task in plan.tasks:
            execute(task)
        finalize(plan)

        collection_path = output / "COLLECTION.json"
        assert collection_path.exists()

        data = json.loads(collection_path.read_text())
        assert "taco:pit_schema" in data

    def test_writes_metadata_parquet(self, flat_a_zip, tmp_path):
        output = tmp_path / "out"
        plan = plan_export(flat_a_zip, output)
        for task in plan.tasks:
            execute(task)
        finalize(plan)

        level0 = output / "METADATA" / "level0.parquet"
        assert level0.exists()

        table = pq.read_table(level0)
        assert table.num_rows == 10

    def test_nested_writes_local_metadata(self, nested_a_zip, tmp_path):
        output = tmp_path / "out"
        plan = plan_export(nested_a_zip, output)
        for task in plan.tasks:
            execute(task)
        finalize(plan)

        meta_files = list(output.rglob("__meta__"))
        assert len(meta_files) == 5

    def test_nested_writes_two_levels(self, nested_a_zip, tmp_path):
        output = tmp_path / "out"
        plan = plan_export(nested_a_zip, output)
        for task in plan.tasks:
            execute(task)
        finalize(plan)

        assert (output / "METADATA" / "level0.parquet").exists()
        assert (output / "METADATA" / "level1.parquet").exists()


class TestFinalizeZip2Folder:

    def test_creates_folder_structure(self, flat_a_zip_path, tmp_path):
        output = tmp_path / "out"
        plan = plan_zip2folder(flat_a_zip_path, output)
        for task in plan.tasks:
            execute(task)

        result = finalize(plan)

        assert result.exists()
        assert (result / "COLLECTION.json").exists()
        assert (result / "METADATA").is_dir()
        assert (result / "DATA").is_dir()

    def test_data_files_exist(self, flat_a_zip_path, tmp_path):
        output = tmp_path / "out"
        plan = plan_zip2folder(flat_a_zip_path, output)
        for task in plan.tasks:
            execute(task)
        finalize(plan)

        data_files = list((output / "DATA").rglob("*"))
        data_files = [f for f in data_files if f.is_file()]
        assert len(data_files) == 10


class TestFinalizeFolder2Zip:

    def test_creates_zip(self, flat_a_folder, tmp_path):
        output = tmp_path / "out.tacozip"
        plan = plan_folder2zip(flat_a_folder, output)

        result = finalize(plan)

        assert result.exists()
        assert result.suffix == ".tacozip"

    def test_zip_is_valid(self, flat_a_folder, tmp_path):
        import tacoreader

        output = tmp_path / "out.tacozip"
        plan = plan_folder2zip(flat_a_folder, output)
        finalize(plan)

        ds = tacoreader.load(output)
        assert ds.pit_schema.root["n"] == 10

    def test_nested_zip_valid(self, nested_a_folder, tmp_path):
        import tacoreader

        output = tmp_path / "out.tacozip"
        plan = plan_folder2zip(nested_a_folder, output)
        finalize(plan)

        ds = tacoreader.load(output)
        assert ds.pit_schema.root["n"] == 5  # 5 folders at level0