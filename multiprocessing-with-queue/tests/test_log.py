# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import pytest

from src.log import log_setup


class TestLog:

    def test_log_setup_should_raise_exception_if_log_level_is_random_string(self):
        with pytest.raises(ValueError):
            log_setup("foo")

    def test_log_setup_should_raise_exception_if_log_level_is_lowercase_log_level(self):
        with pytest.raises(ValueError):
            log_setup("debug")

    def test_log_setup_should_accept_uppercase_log_devel(self):
        log_setup("DEBUG")
