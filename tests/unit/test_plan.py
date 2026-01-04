"""Tests for tacobridge.plan module."""

import pytest

from tacobridge.plan import plan_export, plan_zip2folder, plan_folder2zip
from tacobridge._exceptions import TacoPlanError
from tacobridge._types import ExportPlan, Zip2FolderPlan, Folder2ZipPlan


class TestPlanExport:

    def test_flat_returns_export_plan(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert isinstance(plan, ExportPlan)

    def test_flat_task_count(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert len(plan.tasks) == 10

    def test_flat_single_level(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert len(plan.levels) == 1

    def test_flat_no_local_metadata(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert plan.local_metadata == {}

    def test_nested_task_count(self, nested_a_zip, tmp_path):
        plan = plan_export(nested_a_zip, tmp_path / "out")
        assert len(plan.tasks) == 15  # 5 folders × 3 children

    def test_nested_two_levels(self, nested_a_zip, tmp_path):
        plan = plan_export(nested_a_zip, tmp_path / "out")
        assert len(plan.levels) == 2

    def test_nested_local_metadata(self, nested_a_zip, tmp_path):
        plan = plan_export(nested_a_zip, tmp_path / "out")
        assert len(plan.local_metadata) == 5

    def test_deep_task_count(self, deep_zip, tmp_path):
        plan = plan_export(deep_zip, tmp_path / "out")
        assert len(plan.tasks) == 12  # 3 × 2 × 2

    def test_deep_three_levels(self, deep_zip, tmp_path):
        plan = plan_export(deep_zip, tmp_path / "out")
        assert len(plan.levels) == 3

    def test_filtered_fewer_tasks(self, flat_a_zip, tmp_path):
        filtered = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 50")
        plan = plan_export(filtered, tmp_path / "out")
        assert len(plan.tasks) == 5

    def test_filtered_reindexed(self, flat_a_zip, tmp_path):
        filtered = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover >= 50")
        plan = plan_export(filtered, tmp_path / "out")
        level0 = plan.levels[0]
        current_ids = level0.column("internal:current_id").to_pylist()
        assert current_ids == [0, 1, 2, 3, 4]

    def test_filtered_nested(self, nested_a_zip, tmp_path):
        filtered = nested_a_zip.sql("SELECT * FROM data WHERE cloud_cover < 30")
        plan = plan_export(filtered, tmp_path / "out")
        assert len(plan.tasks) == 6  # 2 folders × 3 children

    def test_output_exists_raises(self, flat_a_zip, tmp_path):
        output = tmp_path / "exists"
        output.mkdir()
        with pytest.raises(TacoPlanError, match="already exists"):
            plan_export(flat_a_zip, output)

    def test_empty_dataset_raises(self, flat_a_zip, tmp_path):
        empty = flat_a_zip.sql("SELECT * FROM data WHERE cloud_cover > 1000")
        with pytest.raises(TacoPlanError, match="empty"):
            plan_export(empty, tmp_path / "out")

    def test_collection_has_pit_schema(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert "taco:pit_schema" in plan.collection

    def test_collection_has_subset_provenance(self, flat_a_zip, tmp_path):
        plan = plan_export(flat_a_zip, tmp_path / "out")
        assert "taco:subset_of" in plan.collection
        assert "taco:subset_date" in plan.collection


class TestPlanZip2Folder:

    def test_returns_zip2folder_plan(self, flat_a_zip_path, tmp_path):
        plan = plan_zip2folder(flat_a_zip_path, tmp_path / "out")
        assert isinstance(plan, Zip2FolderPlan)

    def test_flat_task_count(self, flat_a_zip_path, tmp_path):
        plan = plan_zip2folder(flat_a_zip_path, tmp_path / "out")
        assert len(plan.tasks) == 10

    def test_nested_task_count(self, nested_a_zip_path, tmp_path):
        plan = plan_zip2folder(nested_a_zip_path, tmp_path / "out")
        assert len(plan.tasks) == 15

    def test_strips_zip_columns(self, flat_a_zip_path, tmp_path):
        plan = plan_zip2folder(flat_a_zip_path, tmp_path / "out")
        columns = plan.levels[0].schema.names
        assert "__offset__" not in columns
        assert "__size__" not in columns

    def test_output_exists_raises(self, flat_a_zip_path, tmp_path):
        output = tmp_path / "exists"
        output.mkdir()
        with pytest.raises(TacoPlanError, match="already exists"):
            plan_zip2folder(flat_a_zip_path, output)


class TestPlanFolder2Zip:

    def test_returns_folder2zip_plan(self, flat_a_folder, tmp_path):
        plan = plan_folder2zip(flat_a_folder, tmp_path / "out.tacozip")
        assert isinstance(plan, Folder2ZipPlan)

    def test_flat_entry_count(self, flat_a_folder, tmp_path):
        plan = plan_folder2zip(flat_a_folder, tmp_path / "out.tacozip")
        assert len(plan.entries) == 10

    def test_nested_entry_count(self, nested_a_folder, tmp_path):
        plan = plan_folder2zip(nested_a_folder, tmp_path / "out.tacozip")
        assert len(plan.entries) == 15

    def test_entries_have_arc_paths(self, flat_a_folder, tmp_path):
        plan = plan_folder2zip(flat_a_folder, tmp_path / "out.tacozip")
        for entry in plan.entries:
            assert entry.arc_path.startswith("DATA/")

    def test_output_exists_raises(self, flat_a_folder, tmp_path):
        output = tmp_path / "out.tacozip"
        output.touch()
        with pytest.raises(TacoPlanError, match="already exists"):
            plan_folder2zip(flat_a_folder, output)

    def test_source_not_found_raises(self, tmp_path):
        with pytest.raises(TacoPlanError, match="not found"):
            plan_folder2zip(tmp_path / "nope", tmp_path / "out.tacozip")