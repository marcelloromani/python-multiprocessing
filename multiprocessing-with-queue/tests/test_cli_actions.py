from src.cli_actions import SimpleMsgConsumer
from src.cli_actions import SimpleMsgProducer
from src.cli_actions import get_csv_headers
from src.cli_actions import get_csv_row
from src.config import Config


class TestSimpleMsgProducer:

    def test_should_produce_msg_count_messages(self):
        obj = SimpleMsgProducer(3, 0)
        msg_count = 0
        for msg in obj.yield_msgs():
            msg_count += 1
            assert msg is not None
        assert msg_count == 3


class TestSimpleMsgConsumer:

    def test_should_count_processed_messages(self):
        obj = SimpleMsgConsumer()
        assert obj.processed_message_count == 0
        obj.process_msg({"duration_s": 0})
        assert obj.processed_message_count == 1


class TestCSV:

    def test_headers_length(self):
        config = Config()
        assert len(get_csv_headers(config).split(",")) == 12

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
        assert len(get_csv_row(config, 1).split(",")) == 12
