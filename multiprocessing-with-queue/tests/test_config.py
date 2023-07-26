import pytest
from box.exceptions import BoxKeyError

from src.config import Config


class TestConfigBasicFeatures:

    def test_cannot_access_item_as_dict_if_unset(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            with pytest.raises(BoxKeyError):
                _ = obj[key]

    def test_cannot_access_item_as_dot_notation_if_unset(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            with pytest.raises(BoxKeyError):
                _ = eval(f"obj.{key}")

    def test_can_set_each_item(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            obj[key] = 1

    def test_can_access_item_as_dict_if_set(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            obj[key] = 1

        for key in obj.CONFIG_ITEMS:
            assert obj[key] == 1

    def test_can_access_item_as_dot_notation_if_set(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            obj[key] = 1

        for key in obj.CONFIG_ITEMS:
            assert eval(f"obj.{key}") == 1

    def test_can_overwrite_item(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            obj[key] = 1

        for key in obj.CONFIG_ITEMS:
            obj[key] = 2

        for key in obj.CONFIG_ITEMS:
            assert eval(f"obj.{key}") == 2


class TestConfigLogging:

    def test_should_log_values(self):
        obj = Config()
        for key in obj.CONFIG_ITEMS:
            obj[key] = 1
        obj.log_values()


class TestConfigFactory:

    def test_can_build_config_obj_from_cli_args(self):
        mock_args = Config()
        for key in mock_args.CONFIG_ITEMS:
            mock_args[key] = 1

        obj = Config.from_argparser_args(mock_args)
        assert obj is not None
