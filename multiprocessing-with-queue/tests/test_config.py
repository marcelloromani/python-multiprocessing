import pytest
from box.exceptions import BoxKeyError

from src.config import Config
from .fixtures_utils import fixture_path


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
        for key in obj.CONFIG_ITEMS:
            assert obj[key] == 1

    def test_can_build_config_obj_from_json_str(self):
        json_str = '{ "msg_count": 5 }'
        obj = Config.from_json(json_str)
        assert obj.msg_count == 5

    def test_can_build_config_obj_from_json_file(self):
        json_file = fixture_path('config_test.json')
        obj = Config.from_json(filename=json_file)
        assert obj.msg_count == 3

    def test_can_build_config_obj_from_yaml_str(self):
        yaml_str = 'msg_count: 6'
        obj = Config.from_yaml(yaml_str)
        assert obj.msg_count == 6

    def test_can_build_config_obj_from_yaml_file(self):
        yaml_file = fixture_path('config_test.yaml')
        obj = Config.from_yaml(filename=yaml_file)
        assert obj.msg_count == 7


class TestConfigCSV:

    def test_header_count(self):
        config = Config()
        headers = config.csv_headers()
        assert len(headers.split(",")) == 12

    def test_row_field_count(self):
        config = Config()
        config.msg_count = 1
        config.task_duration_sec = 1
        config.queue_max_size = 1
        config.consumer_count = 1
        config.queue_put_timeout_sec = 0
        config.queue_full_max_attempts = 0
        config.queue_full_wait_sec = 0
        config.queue_get_timeout_sec = 0
        config.queue_empty_max_attempts = 0
        config.queue_empty_wait_sec = 0

        row_string = config.csv_row(elapsed_sec=1.0)
        assert len(row_string.split(",")) == 12
