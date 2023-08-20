import logging
import queue
from multiprocessing import Queue
from time import sleep
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

    def put(self, msg_queue: Queue, msg_type: str, msg: str):
        attempts: int = 0

        while attempts < self._max_attempts:
            try:
                self.logger.debug("Trying to enqueue %s %s attempts=%d", msg_type, msg, attempts)

                attempts += 1
                msg_queue.put((msg_type, msg), block=True, timeout=self._timeout)

                self.logger.debug("Enqueued %s %s after %d attempts", msg_type, msg, attempts)
                break
            except TimeoutError as ex:
                self.logger.error("TimeoutError: %s", ex)
                raise
            except queue.Full as ex:
                self.logger.debug("queue.Full: %s attempts: %d", ex, attempts)

                if attempts < self._max_attempts:
                    self.logger.debug("Sleeping %f sec before next attempt", self._wait_between_attempts)
                    sleep(self._wait_between_attempts)
                else:
                    self.logger.error("Reached max attempts %d", self._max_attempts)
                    raise
