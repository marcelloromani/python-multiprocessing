from src.cli_actions import SimpleMsgProducer
from src.cli_actions import SimpleMsgConsumer


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
