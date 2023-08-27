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
        attempts: int = 0

        while attempts < self.max_attempts:
            try:
                attempts += 1
                return self._process(msg_queue, attempts)
            except TimeoutError as ex:
                self.logger.error("TimeoutError: %s", ex)
                raise
            except queue.Empty as ex:
                self.logger.debug("queue.Empty: %s attempts: %d", ex, attempts)

                if attempts < self.max_attempts:
                    self.logger.debug("Sleeping %f sec before next attempt", self.wait_between_attempts)
                    sleep(self.wait_between_attempts)
                else:
                    self.logger.error("Reached max attempts %d", self.max_attempts)
                    raise

    def _process(self, msg_queue, attempts):
        self.logger.debug("Trying to dequeue message, attempts=%d", attempts)
        msg_type, msg = msg_queue.get(block=True, timeout=self.timeout)
        self.logger.debug("Dequeued %s %s after %d attempts", msg_type, msg, attempts)
        return msg_type, msg
