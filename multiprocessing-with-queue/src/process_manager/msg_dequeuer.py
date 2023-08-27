import logging
import queue
from multiprocessing import Queue
from time import sleep
from .msg_processor import MsgProcessor


class MsgDequeuer(MsgProcessor):
    logger = logging.getLogger("MsgDequeuer")

    def __init__(self, timeout: float = 0, max_attempts: int = 1, wait_between_attempts: float = 0):
        """
        :param timeout: queue.get(): how long to wait before timing out
        :param max_attempts: how many times to retry on timeout
        :param wait_between_attempts: before trying another get() after timeout, wait these many seconds
        """
        super().__init__(timeout, max_attempts, wait_between_attempts)

    def get(self, msg_queue: Queue) -> (str, str):
        return self._run_with_retry(self._process, msg_queue)

    def _process(self, msg_queue):
        self.logger.debug("Trying to dequeue message")
        msg_type, msg = msg_queue.get(block=True, timeout=self.timeout)
        self.logger.debug("Dequeued %s %s", msg_type, msg)
        return msg_type, msg
