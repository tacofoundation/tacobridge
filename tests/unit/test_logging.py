"""Tests for tacobridge._logging module."""

import logging

from tacobridge._logging import get_logger, setup_basic_logging, disable_logging


class TestGetLogger:

    def test_returns_logger(self):
        logger = get_logger("test")
        assert isinstance(logger, logging.Logger)

    def test_prefixes_name(self):
        logger = get_logger("mymodule")
        assert logger.name == "tacobridge.mymodule"

    def test_already_prefixed(self):
        logger = get_logger("tacobridge.existing")
        assert logger.name == "tacobridge.existing"

    def test_main_becomes_tacobridge(self):
        logger = get_logger("__main__")
        assert logger.name == "tacobridge"


class TestSetupBasicLogging:

    def test_sets_level(self):
        setup_basic_logging(level=logging.DEBUG)
        logger = logging.getLogger("tacobridge")
        assert logger.level == logging.DEBUG

    def test_adds_handler(self):
        logger = logging.getLogger("tacobridge")
        logger.handlers.clear()

        setup_basic_logging()

        assert len(logger.handlers) >= 1

    def test_no_duplicate_handlers(self):
        logger = logging.getLogger("tacobridge")
        logger.handlers.clear()

        setup_basic_logging()
        setup_basic_logging()

        assert len(logger.handlers) == 1


class TestDisableLogging:

    def test_disables_all(self):
        disable_logging()
        logger = logging.getLogger("tacobridge")
        assert logger.level > logging.CRITICAL