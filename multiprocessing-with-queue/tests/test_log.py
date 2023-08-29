# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import logging

import pytest

from src.log import log_setup


class TestLog:
    logger = logging.getLogger("TestLog")
    current_log_level = None

    @classmethod
    def setup_class(cls):
        # store log level before running tests
        cls.current_log_level = cls.logger.getEffectiveLevel()

    @classmethod
    def teardown_class(cls):
        # restore log level previous to tests
        log_setup(cls.current_log_level)

    def test_log_setup_should_raise_exception_if_log_level_is_random_string(self):
        with pytest.raises(ValueError):
            log_setup("foo")

    def test_log_setup_should_accept_lowercase_log_level(self):
        log_setup("debug")

    def test_log_setup_should_accept_uppercase_log_devel(self):
        log_setup("DEBUG")
