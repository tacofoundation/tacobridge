"""Tests for tacobridge._types module."""

import pytest
from pathlib import Path

import pyarrow as pa

from tacobridge._types import (
    CopyTask,
    ZipEntry,
    ExportPlan,
    Zip2FolderPlan,
    Folder2ZipPlan,
)


class TestCopyTask:

    def test_create_minimal(self):
        task = CopyTask(src="/a", dest="/b")
        assert task.src == "/a"
        assert task.dest == "/b"
        assert task.offset is None
        assert task.size is None

    def test_create_with_offset_size(self):
        task = CopyTask(src="/a", dest="/b", offset=100, size=50)
        assert task.offset == 100
        assert task.size == 50

    def test_frozen(self):
        task = CopyTask(src="/a", dest="/b")
        with pytest.raises(Exception):
            task.src = "/c"

    def test_hashable(self):
        task = CopyTask(src="/a", dest="/b", offset=0, size=10)
        assert hash(task) is not None
        assert {task}  # can be in set


class TestZipEntry:

    def test_create(self):
        entry = ZipEntry(src="/local/file.tif", arc_path="DATA/sample/file.tif")
        assert entry.src == "/local/file.tif"
        assert entry.arc_path == "DATA/sample/file.tif"

    def test_frozen(self):
        entry = ZipEntry(src="/a", arc_path="b")
        with pytest.raises(Exception):
            entry.arc_path = "c"


class TestExportPlan:

    def test_create_minimal(self):
        plan = ExportPlan(
            tasks=(),
            source="/src",
            output=Path("/out"),
        )
        assert plan.tasks == ()
        assert plan.levels == ()
        assert plan.local_metadata == {}
        assert plan.collection == {}

    def test_with_levels(self):
        table = pa.table({"id": ["a"]})
        plan = ExportPlan(
            tasks=(),
            source="/src",
            output=Path("/out"),
            levels=(table,),
        )
        assert len(plan.levels) == 1

    def test_frozen(self):
        plan = ExportPlan(tasks=(), source="/src", output=Path("/out"))
        with pytest.raises(Exception):
            plan.source = "/new"


class TestZip2FolderPlan:

    def test_create(self):
        plan = Zip2FolderPlan(
            tasks=(),
            source="/src.tacozip",
            output=Path("/out"),
        )
        assert plan.source == "/src.tacozip"

    def test_frozen(self):
        plan = Zip2FolderPlan(tasks=(), source="/src", output=Path("/out"))
        with pytest.raises(Exception):
            plan.output = Path("/new")


class TestFolder2ZipPlan:

    def test_create(self):
        plan = Folder2ZipPlan(
            entries=(),
            source=Path("/src"),
            output=Path("/out.tacozip"),
        )
        assert plan.entries == ()

    def test_with_entries(self):
        entry = ZipEntry(src="/a", arc_path="DATA/a")
        plan = Folder2ZipPlan(
            entries=(entry,),
            source=Path("/src"),
            output=Path("/out.tacozip"),
        )
        assert len(plan.entries) == 1

    def test_frozen(self):
        plan = Folder2ZipPlan(entries=(), source=Path("/src"), output=Path("/out"))
        with pytest.raises(Exception):
            plan.entries = ()