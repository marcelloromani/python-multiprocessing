import pytest

from src.cli_actions import SimpleMsgConsumer
from src.cli_actions import SimpleMsgProducer
from src.cli_actions import run_session
from src.cli_actions import run_single
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


class TestRuns:

    def test_should_run_single(self):
        config = Config()
        config.msg_count = 1
        config.task_duration_sec = 0
        config.queue_max_size = 1
        config.consumer_count = 1
        config.queue_put_timeout_sec = 0
        config.queue_full_max_attempts = 5
        config.queue_full_wait_sec = 1
        config.queue_get_timeout_sec = 1
        config.queue_empty_max_attempts = 5
        config.queue_empty_wait_sec = 0
        run_single(config)

    def test_should_run_session(self):
        config = Config()
        config.msg_count = 1
        config.task_duration_sec = 0
        config.queue_max_size = 1
        config.consumer_count = 1
        config.queue_put_timeout_sec = 0
        config.queue_full_max_attempts = 5
        config.queue_full_wait_sec = 1
        config.queue_get_timeout_sec = 1
        config.queue_empty_max_attempts = 5
        config.queue_empty_wait_sec = 0
        consumer_min = 1
        consumer_max = 2
        consumer_step = 1
        run_session(config, consumer_min, consumer_max, consumer_step)

    def test_should_fail_if_run_session_and_consumer_step_zero(self):
        config = Config()
        config.msg_count = 1
        config.task_duration_sec = 0
        config.queue_max_size = 1
        config.consumer_count = 1
        config.queue_put_timeout_sec = 0
        config.queue_full_max_attempts = 5
        config.queue_full_wait_sec = 1
        config.queue_get_timeout_sec = 1
        config.queue_empty_max_attempts = 5
        config.queue_empty_wait_sec = 0
        consumer_min = 1
        consumer_max = 2
        consumer_step = 0
        with pytest.raises(ValueError):
            run_session(config, consumer_min, consumer_max, consumer_step)
