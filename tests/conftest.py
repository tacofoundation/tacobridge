"""Shared fixtures for tacobridge tests."""

import pytest
from pathlib import Path

import tacoreader

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def flat_a_zip():
    return tacoreader.load(FIXTURES / "zip/flat_a/flat_a.tacozip")


@pytest.fixture
def flat_b_zip():
    return tacoreader.load(FIXTURES / "zip/flat_b/flat_b.tacozip")


@pytest.fixture
def nested_a_zip():
    return tacoreader.load(FIXTURES / "zip/nested_a/nested_a.tacozip")


@pytest.fixture
def nested_b_zip():
    return tacoreader.load(FIXTURES / "zip/nested_b/nested_b.tacozip")


@pytest.fixture
def deep_zip():
    return tacoreader.load(FIXTURES / "zip/deep/deep.tacozip")


@pytest.fixture
def flat_a_folder():
    return FIXTURES / "folder/flat_a"


@pytest.fixture
def nested_a_folder():
    return FIXTURES / "folder/nested_a"


@pytest.fixture
def flat_a_zip_path():
    return FIXTURES / "zip/flat_a/flat_a.tacozip"


@pytest.fixture
def nested_a_zip_path():
    return FIXTURES / "zip/nested_a/nested_a.tacozip"