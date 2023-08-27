import queue
from time import sleep
import pytest
from box import Box
from unittest import mock
from src.process_manager.msg_processor import MsgProcessor


class TestMsgProcessorRetryLogic:

    @classmethod
    def no_exception(cls, ctx: Box):
        ctx.call_count += 1

    @classmethod
    def raise_timeout_once(cls, ctx: Box):
        ctx.call_count += 1
        if ctx.call_count == 1:
            raise TimeoutError

    @classmethod
    def raise_queue_full_once(cls, ctx: Box):
        ctx.call_count += 1
        if ctx.call_count == 1:
            raise queue.Full

    @classmethod
    def raise_queue_empty_once(cls, ctx: Box):
        ctx.call_count += 1
        if ctx.call_count == 1:
            raise queue.Empty

    @classmethod
    def multiply(cls, value: int) -> int:
        return value * 2

    # happy path

    def test_should_call_func_once_if_no_exception(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=1,
            wait_between_attempts=1,
        )
        ctx = Box()
        ctx.call_count = 0
        obj._run_with_retry(self.no_exception, ctx)
        assert ctx.call_count == 1

    # timeout exception

    def test_should_fail_if_only_one_attempt_and_timeout(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=1,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        with pytest.raises(TimeoutError):
            obj._run_with_retry(self.raise_timeout_once, ctx)

    def test_should_not_fail_if_two_attempts_and_timeout(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=2,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        obj._run_with_retry(self.raise_timeout_once, ctx)
        assert ctx.call_count == 2

    # queue full exception

    def test_should_fail_if_only_one_attempt_and_queue_full(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=1,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        with pytest.raises(queue.Full):
            obj._run_with_retry(self.raise_queue_full_once, ctx)

    def test_should_not_fail_if_two_attempts_and_queue_full(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=2,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        obj._run_with_retry(self.raise_queue_full_once, ctx)
        assert ctx.call_count == 2

    # queue empty exception

    def test_should_fail_if_only_one_attempt_and_queue_empty(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=1,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        with pytest.raises(queue.Empty):
            obj._run_with_retry(self.raise_queue_empty_once, ctx)

    def test_should_not_fail_if_two_attempts_and_queue_empty(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=2,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        obj._run_with_retry(self.raise_queue_empty_once, ctx)
        assert ctx.call_count == 2

    # return value form the wrapped function

    def test_should_relay_func_return_value(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=1,
            wait_between_attempts=0,
        )
        retval = obj._run_with_retry(self.multiply, 3)
        assert retval == 6

    # sleeping between attempts

    def test_if_exception_should_wait_before_next_attempt(self):
        obj = MsgProcessor(
            timeout=0,
            max_attempts=2,
            wait_between_attempts=0,
        )
        ctx = Box()
        ctx.call_count = 0
        with mock.patch('time.sleep') as mock_sleep:
            obj._run_with_retry(self.raise_queue_empty_once, ctx)
            assert ctx.call_count == 2
            assert mock_sleep.call_count == 1
