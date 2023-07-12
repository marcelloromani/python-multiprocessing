import queue
from time import sleep

import pytest

from src.process_manager import MsgProducer, MsgConsumer, ProcessManager


class CountingMsgProducer(MsgProducer):

    def __init__(self, num_of_msg_to_produce: int):
        self._produced_msgs = []
        self._num_of_msg_to_produce: int = num_of_msg_to_produce

    def yield_msg(self):
        for i in range(self._num_of_msg_to_produce):
            msg = {
                "msg_id": i,
            }
            self._produced_msgs.append(msg)
            yield msg

    @property
    def produced_msgs(self) -> list:
        return self._produced_msgs


class CountingMsgConsumer(MsgConsumer):

    def __init__(self):
        self._processed_msg_count = 0

    def process_msg(self, msg):
        self._processed_msg_count += 1

    @property
    def processed_msg_count(self):
        return self._processed_msg_count


class LongProcessMsgConsumer(MsgConsumer):

    def __init__(self, process_msg_duration_s: float):
        """
        :param process_msg_duration_s: how long the dummy process msg method should wait
        """
        self._process_msg_duration_s = process_msg_duration_s

    def process_msg(self, msg):
        sleep(self._process_msg_duration_s)


class TestCountingMsgProducer:

    def test_msg_producer_should_return_empty_list_on_zero_msgs(self):
        s = CountingMsgProducer(0)
        assert s.produced_msgs == []

    def test_msg_producer_should_report_on_one_msg(self):
        s = CountingMsgProducer(1)
        actual = []
        for msg in s.yield_msg():
            actual.append(msg)
        assert len(actual) == 1
        assert len(s.produced_msgs) == 1
        assert actual == s.produced_msgs

    def test_msg_producer_should_report_on_five_msg(self):
        s = CountingMsgProducer(5)
        actual = []
        for msg in s.yield_msg():
            actual.append(msg)
        assert len(actual) == 5
        assert len(s.produced_msgs) == 5
        assert actual == s.produced_msgs


class TestCountingMsgConsumer:

    def test_msg_consumer_should_return_empty_list_on_zero_msgs(self):
        s = CountingMsgConsumer()
        assert s.processed_msg_count == 0

    def test_msg_consumer_should_report_on_one_msg(self):
        s = CountingMsgConsumer()
        actual = []
        for i in range(1):
            actual.append(i)
            s.process_msg(i)
        assert len(actual) == 1
        assert s.processed_msg_count == 1

    def test_msg_consumer_should_report_on_five_msg(self):
        s = CountingMsgConsumer()
        actual = []
        for i in range(5):
            actual.append(i)
            s.process_msg(i)
        assert len(actual) == 5
        assert s.processed_msg_count == 5


class TestProcessManager:

    def test_one_msg_one_producer(self):
        src = CountingMsgProducer(1)
        dest = CountingMsgConsumer()
        pm = ProcessManager(queue_max_size=1)
        pm.process(src, dest, worker_count=1)

    def test_should_raise_queue_full_if_we_fill_the_queue(self):
        src = CountingMsgProducer(1)
        dest = CountingMsgConsumer()
        pm = ProcessManager(queue_max_size=1, queue_timeout_s=0)
        with pytest.raises(queue.Full):
            pm.process(src, dest, worker_count=1)
