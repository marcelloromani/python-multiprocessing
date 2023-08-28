import logging
from multiprocessing import Queue

from .msg_processor import MsgProcessor


class MsgEnqueuer(MsgProcessor):
    logger = logging.getLogger("MsgEnqueuer")

    def __init__(self, timeout: float = 0, max_attempts: int = 1, wait_between_attempts: float = 0):
        """
        :param timeout: queue.put(): how long to wait before timing out
        :param max_attempts: how many times to retry on timeout
        :param wait_between_attempts: before trying another put() after timeout, wait these many seconds
        """
        super().__init__(timeout, max_attempts, wait_between_attempts)

    def _process(self, msg_queue, msg_type, msg):
        self.logger.debug("Trying to enqueue %s %s", msg_type, msg)
        msg_queue.put((msg_type, msg), block=True, timeout=self.timeout)
        self.logger.debug("Enqueued %s %s", msg_type, msg)

    def put(self, msg_queue: Queue, msg_type: str, msg: str):
        return self._run_with_retry(self._process, msg_queue, msg_type, msg)
