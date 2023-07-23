import queue
from multiprocessing import Queue
from time import sleep

import pytest

from src.process_manager import MsgConsumer
from src.process_manager import MsgDequeuer
from src.process_manager import MsgEnqueuer
from src.process_manager import MsgProducer
from src.process_manager import ProcessManager


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


class TestMsgDequeuer:

    def test_with_defaults_should_raise_queue_empty_if_no_data(self):
        q = Queue()
        obj = MsgDequeuer()
        with pytest.raises(queue.Empty):
            _ = obj.get(q)

    def test_should_try_max_attempts_times(self):
        q = Queue()
        obj = MsgDequeuer(max_attempts=5)
        with pytest.raises(queue.Empty):
            _ = obj.get(q)


class TestMsgEnqueuer:

    def test_with_defaults_should_raise_queue_full_if_no_consumers(self):
        q = Queue(maxsize=1)
        obj = MsgEnqueuer()
        obj.put(q, "foo", "first message")
        with pytest.raises(queue.Full):
            obj.put(q, "bar", "second messagse")

    def test_should_try_max_attempts_times(self):
        q = Queue(maxsize=1)
        obj = MsgEnqueuer(max_attempts=5)
        obj.put(q, "foo", "first message")
        with pytest.raises(queue.Full):
            obj.put(q, "bar", "second messagse")


class TestProcessManager:

    def test_should_be_able_to_produce_consume_one_msg_with_default_values(self):
        src = CountingMsgProducer(1)
        dest = CountingMsgConsumer()
        enqueuer = MsgEnqueuer()
        dequeuer = MsgDequeuer()
        pm = ProcessManager(enqueuer=enqueuer, dequeuer=dequeuer)
        pm.process(src, dest, consumer_count=1)

    def test_should_raise_queue_full_if_we_fill_the_queue(self):
        src = CountingMsgProducer(1)
        dest = CountingMsgConsumer()
        enqueuer = MsgEnqueuer()
        dequeuer = MsgDequeuer()
        pm = ProcessManager(enqueuer=enqueuer, dequeuer=dequeuer, queue_max_size=1)
        with pytest.raises(queue.Full):
            pm.process(src, dest, consumer_count=1)
