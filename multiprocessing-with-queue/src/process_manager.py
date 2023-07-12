"""
Connects a message source and a number of message sinks through a queue.
"""
import logging
import os
import queue
from multiprocessing import Queue, Process
from time import sleep

from src.log import log_setup


class MsgSource:
    """Message Source (producer) interface"""

    def get_msg(self):
        """Yield all messages"""
        raise NotImplementedError()


class MsgSink:
    """Message Sinck (consumer) interface"""

    def process_msg(self, msg):
        """Process a single message"""
        raise NotImplementedError()


class ProcessManager:
    """
    Connects a message source and a number of message sinks through a queue.
    """

    MSG_TYPE_USER: str = "USER"
    MSG_TYPE_QUIT: str = "QUIT"

    logger = logging.getLogger("ProcessManager")

    def __init__(self, queue_max_size: int, queue_timeout_s: int = 10, queue_full_max_attempts: int = 1,
                 queue_empty_max_attempts: int = 1,
                 mermaid_diagram: bool = False, log_level: int = logging.ERROR):
        """
        :param queue_max_size: maximum size of the internal queue
        :param queue_timeout_s: how long to wait for a q.get() or q.put() to succeed before declaring it failed
        :param queue_full_max_attempts: retry when q.put() fails due to Queue Full
        :param queue_empty_max_attempts: retry when q.get() fails due to Queue Empty
        :param mermaid_diagram: if True, print Mermaid-compatible sequenceDiagram directives
        """
        self._q = Queue(queue_max_size)
        self._queue_timeout = queue_timeout_s
        self._queue_full_max_attempts = queue_full_max_attempts
        self._queue_empty_max_attempts = queue_empty_max_attempts
        self._mermaid_diagram = mermaid_diagram
        self._log_level = log_level

        self.logger.debug(
            "queue_max_size: %d queue_timeout_s: %d queue_full_max_attempts: %d queue_empty_max_attempts: %d",
            queue_max_size,
            queue_timeout_s,
            queue_full_max_attempts,
            queue_empty_max_attempts,
        )

    def _enqueue_msg(self, msg_queue: Queue, msg_type: str, msg: str, timeout: int):
        # how many times should we try to enqueue a message if the queue is full?
        WAIT_BEFORE_NEXT_ATTEMPT_S: float = 1
        attempts: int = 0

        while attempts < self._queue_full_max_attempts:
            try:
                attempts += 1
                self.logger.debug("Trying to enqueue %s %s attempts=%d", msg_type, msg, attempts)
                msg_queue.put((msg_type, msg), block=True, timeout=timeout)

                if self._mermaid_diagram:
                    proc_id = f"Proc.{os.getpid()}"
                    print(f"    {proc_id} ->> Queue: \"{msg_type} {msg}\"")

                self.logger.debug("Enqueued %s %s", msg_type, msg)
                break
            except TimeoutError as ex:
                self.logger.error("TimeoutError: %s", ex)
                raise
            except queue.Full as ex:
                self.logger.debug("queue.Full: %s attempts: %d", ex, attempts)
                if attempts < self._queue_full_max_attempts:
                    self.logger.debug("Sleeping %d sec before next attempt", WAIT_BEFORE_NEXT_ATTEMPT_S)

                    if self._mermaid_diagram:
                        proc_id = f"Proc.{os.getpid()}"
                        print(f"    {proc_id} ->> {proc_id}: queueFull({attempts})")

                    sleep(WAIT_BEFORE_NEXT_ATTEMPT_S)
                else:
                    raise

    def _dequeue_msg(self, msg_queue: Queue, timeout: int):
        # how many times should we try to dequeue a message if the queue is empty?
        WAIT_BEFORE_NEXT_ATTEMPT_S: float = 1
        attempts: int = 0

        while attempts < self._queue_empty_max_attempts:
            try:
                attempts += 1
                self.logger.debug("trying to dequeue message")
                msg_type, msg = msg_queue.get(block=True, timeout=timeout)

                if self._mermaid_diagram:
                    proc_id = f"Proc.{os.getpid()}"
                    print(f"    Queue ->> {proc_id}: \"{msg_type} {msg}\"")

                self.logger.debug("dequeued %s %s", msg_type, msg)
                return msg_type, msg
            except TimeoutError as ex:
                self.logger.error("TimeoutError: %s", ex)
                raise
            except queue.Empty as ex:
                self.logger.debug("queue.Empty: %s attempts: %d", ex, attempts)
                if attempts < self._queue_empty_max_attempts:
                    self.logger.debug("Sleeping %d sec before next attempt", WAIT_BEFORE_NEXT_ATTEMPT_S)

                    if self._mermaid_diagram:
                        proc_id = f"Proc.{os.getpid()}"
                        print(f"    {proc_id} ->> {proc_id}: queueEmpty({attempts})")

                    sleep(WAIT_BEFORE_NEXT_ATTEMPT_S)
                else:
                    raise
        raise RuntimeError(f"Dequeue failed after {attempts} attempts")

    def process(self, msg_source: MsgSource, msg_sink: MsgSink, worker_count: int):
        self.logger.debug("start")

        if self._mermaid_diagram:
            print("sequenceDiagram")

        # create worker pool
        workers = []
        for worker_index in range(worker_count):
            self.logger.debug("Creating worker process %d", worker_index)
            worker_process = Process(target=self._dequeue_and_process_msg, args=(msg_sink,))
            workers.append(worker_process)
            worker_process.start()

        for msg in msg_source.get_msg():
            self._enqueue_msg(self._q, self.MSG_TYPE_USER, msg, self._queue_timeout)

        self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)

        for worker_process in workers:
            self.logger.debug("Joining worker process %d", worker_process.pid)

            if self._mermaid_diagram:
                # this process sends a Join() request to the worker process...
                proc_id = f"Proc.{os.getpid()}"
                worker_id = f"Proc.{worker_process.pid}"
                print(f"    {proc_id} ->> {worker_id}: p.Join")

            worker_process.join()

            if self._mermaid_diagram:
                # ...the worker process acknowledges the Join() request
                proc_id = f"Proc.{os.getpid()}"
                worker_id = f"Proc.{worker_process.pid}"
                print(f"    {worker_id} ->> {proc_id}: p.Join")

        self.logger.debug("end")

    def _dequeue_and_process_msg(self, msg_sink: MsgSink):
        # we're on a new process, sys.stdout is different from our parent process
        log_setup(self._log_level)
        self.logger = logging.getLogger("Dequeuer")

        self.logger.debug("start")

        terminate = False

        while not terminate:

            msg_type, msg = self._dequeue_msg(self._q, self._queue_timeout)

            if msg_type:
                if msg_type == self.MSG_TYPE_USER:
                    self.logger.debug("processing %s %s", msg_type, msg)
                    msg_sink.process_msg(msg)
                elif msg_type == self.MSG_TYPE_QUIT:
                    self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)
                    terminate = True
                else:
                    raise ValueError(f"Unexpected message type {msg_type}")

        self.logger.debug("end")
