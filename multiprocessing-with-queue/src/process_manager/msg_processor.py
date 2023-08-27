import logging

import queue
class MsgProcessor:
    logger = logging.getLogger()

    def __init__(self, timeout: float = 0, max_attempts: int = 1, wait_between_attempts: float = 0):
        """
        :param timeout: how long to wait before timing out
        :param max_attempts: how many times to retry on timeout
        :param wait_between_attempts: before trying another put() after timeout, wait these many seconds
        """
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._wait_between_attempts = wait_between_attempts

    @property
    def timeout(self):
        return self._timeout

    @property
    def max_attempts(self):
        return self._max_attempts

    @property
    def wait_between_attempts(self):
        return self._wait_between_attempts

    def _run_with_retry(self, func, *args, **kwargs):
        attempts: int = 0

        while attempts < self.max_attempts:
            try:
                attempts += 1
                return func(*args, **kwargs)
            except TimeoutError as ex:
                self.logger.error("TimeoutError: %s Attempts: %d", ex, attempts)
                if attempts >= self.max_attempts:
                    raise

            except queue.Full as ex:
                self.logger.error("Queue Full: %s Attempts: %d", ex, attempts)
                if attempts >= self.max_attempts:
                    raise

            except queue.Empty as ex:
                self.logger.error("Queue Empty: %s Attempts: %d", ex, attempts)
                if attempts >= self.max_attempts:
                    raise
