"""Tests for tacobridge.execute module."""

import pytest

from tacobridge.execute import execute, _read_bytes, _write_bytes
from tacobridge._types import CopyTask
from tacobridge._exceptions import TacoExecuteError


class TestExecute:

    def test_copy_full_file(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"hello world")
        dest = tmp_path / "dest.bin"

        task = CopyTask(src=str(src), dest=str(dest), offset=None, size=None)
        execute(task)

        assert dest.read_bytes() == b"hello world"

    def test_copy_with_offset(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"hello world")
        dest = tmp_path / "dest.bin"

        task = CopyTask(src=str(src), dest=str(dest), offset=6, size=5)
        execute(task)

        assert dest.read_bytes() == b"world"

    def test_copy_with_offset_zero(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"hello world")
        dest = tmp_path / "dest.bin"

        task = CopyTask(src=str(src), dest=str(dest), offset=0, size=5)
        execute(task)

        assert dest.read_bytes() == b"hello"

    def test_creates_parent_dirs(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"data")
        dest = tmp_path / "a" / "b" / "c" / "dest.bin"

        task = CopyTask(src=str(src), dest=str(dest), offset=None, size=None)
        execute(task)

        assert dest.read_bytes() == b"data"

    def test_src_not_found_raises(self, tmp_path):
        task = CopyTask(
            src=str(tmp_path / "nope.bin"),
            dest=str(tmp_path / "dest.bin"),
            offset=None,
            size=None,
        )
        with pytest.raises(TacoExecuteError):
            execute(task)

    def test_dest_permission_denied_raises(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"data")
        dest = tmp_path / "readonly" / "dest.bin"
        dest.parent.mkdir()
        dest.parent.chmod(0o444)

        task = CopyTask(src=str(src), dest=str(dest), offset=None, size=None)

        try:
            with pytest.raises(TacoExecuteError):
                execute(task)
        finally:
            dest.parent.chmod(0o755)


class TestReadBytes:

    def test_read_full(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"0123456789")

        data = _read_bytes(str(src), None, None)
        assert data == b"0123456789"

    def test_read_with_offset_and_size(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"0123456789")

        data = _read_bytes(str(src), 3, 4)
        assert data == b"3456"

    def test_read_offset_only(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"0123456789")

        data = _read_bytes(str(src), 5, None)
        assert data == b"56789"


class TestWriteBytes:

    def test_write_creates_file(self, tmp_path):
        dest = tmp_path / "new.bin"
        _write_bytes(str(dest), b"content")
        assert dest.read_bytes() == b"content"

    def test_write_overwrites(self, tmp_path):
        dest = tmp_path / "existing.bin"
        dest.write_bytes(b"old")
        _write_bytes(str(dest), b"new")
        assert dest.read_bytes() == b"new"

    def test_write_creates_parents(self, tmp_path):
        dest = tmp_path / "deep" / "nested" / "file.bin"
        _write_bytes(str(dest), b"deep")
        assert dest.read_bytes() == b"deep"