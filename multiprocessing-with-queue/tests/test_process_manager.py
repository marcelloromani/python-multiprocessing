import queue
from time import sleep

import pytest

from src.process_manager import MsgSource, MsgSink, ProcessManager


class CountMsgSource(MsgSource):

    def __init__(self, num_of_desired_msg: int):
        self._produced_msgs = []
        self._num_of_desired_msg: int = num_of_desired_msg

    def get_msg(self):
        for i in range(self._num_of_desired_msg):
            msg = {
                "msg_id": i,
            }
            self._produced_msgs.append(msg)
            yield msg

    @property
    def produced_msgs(self) -> list:
        return self._produced_msgs


class CountMsgSink(MsgSink):

    def __init__(self):
        self._processed_msg_count = 0

    def process_msg(self, msg):
        self._processed_msg_count += 1

    @property
    def processed_msg_count(self):
        return self._processed_msg_count


class LongProcessMsgSink(MsgSink):

    def __init__(self, process_msg_duration_s: float):
        """
        :param process_msg_duration_s: how long the dummy process msg method should wait
        """
        self._process_msg_duration_s = process_msg_duration_s

    def process_msg(self, msg):
        sleep(self._process_msg_duration_s)


class TestCountMsgSource:

    def test_msg_producer_should_return_empty_list_on_zero_msgs(self):
        s = CountMsgSource(0)
        assert s.produced_msgs == []

    def test_msg_producer_should_report_on_one_msg(self):
        s = CountMsgSource(1)
        actual = []
        for msg in s.get_msg():
            actual.append(msg)
        assert len(actual) == 1
        assert len(s.produced_msgs) == 1
        assert actual == s.produced_msgs

    def test_msg_producer_should_report_on_five_msg(self):
        s = CountMsgSource(5)
        actual = []
        for msg in s.get_msg():
            actual.append(msg)
        assert len(actual) == 5
        assert len(s.produced_msgs) == 5
        assert actual == s.produced_msgs


class TestCountMsgSink:

    def test_msg_consumer_should_return_empty_list_on_zero_msgs(self):
        s = CountMsgSink()
        assert s.processed_msg_count == 0

    def test_msg_consumer_should_report_on_one_msg(self):
        s = CountMsgSink()
        actual = []
        for i in range(1):
            actual.append(i)
            s.process_msg(i)
        assert len(actual) == 1
        assert s.processed_msg_count == 1

    def test_msg_consumer_should_report_on_five_msg(self):
        s = CountMsgSink()
        actual = []
        for i in range(5):
            actual.append(i)
            s.process_msg(i)
        assert len(actual) == 5
        assert s.processed_msg_count == 5


class TestMsgBaseClasses:

    def test_msg_source_is_abstract(self):
        with pytest.raises(NotImplementedError):
            s = MsgSource()
            s.get_msg()

    def test_msg_sink_is_abstract(self):
        with pytest.raises(NotImplementedError):
            s = MsgSink()
            s.process_msg({})


class TestProcessManager:

    def test_one_msg_one_producer(self):
        src = CountMsgSource(1)
        dest = CountMsgSink()
        pm = ProcessManager(queue_max_size=1)
        pm.process(src, dest, worker_count=1)

    def test_should_raise_queue_full_if_we_fill_the_queue(self):
        src = CountMsgSource(1)
        dest = CountMsgSink()
        pm = ProcessManager(queue_max_size=1, queue_timeout_s=0)
        with pytest.raises(queue.Full):
            pm.process(src, dest, worker_count=1)

    def _should_be_a_mock(self):
        self._called = True

    def test_should_call_init_handler(self):
        src = CountMsgSource(1)
        dest = CountMsgSink()
        pm = ProcessManager(queue_max_size=1, queue_timeout_s=0)
        pm.add_init_listener(self._should_be_a_mock())
        pm.process(src, dest, worker_count=1)
        assert self._called
