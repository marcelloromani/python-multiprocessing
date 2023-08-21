import pytest

from src.process_manager import MsgConsumer, MsgProducer


class TestBaseClasses:

    def test_msg_producer_is_abstract(self):
        with pytest.raises(NotImplementedError):
            obj = MsgProducer()
            for _ in obj.yield_msgs():
                pass

    def test_msg_consumer_is_abstract(self):
        with pytest.raises(NotImplementedError):
            obj = MsgConsumer()
            obj.process_msg({})
