import os
import queue
from multiprocessing import Queue, Process
from time import sleep

from src.log import log_debug


class MsgSource:

    def get_msg(self):
        raise NotImplementedError()


class MsgSink:

    def process_msg(self, msg):
        raise NotImplementedError()


class ProcessManager:
    MSG_TYPE_USER: str = "USER"
    MSG_TYPE_QUIT: str = "QUIT"

    def __init__(self, queue_max_size: int, queue_timeout_s: int = 10, queue_full_max_attempts: int = 1,
                 queue_empty_max_attempts: int = 1,
                 mermaid_diagram: bool = False):
        """
        :param queue_max_size: maximum size of the internal queue
        :param queue_timeout_s: how long to wait for a q.get() or q.put() to succeed before declaring it failed
        :param queue_full_max_attempts: retry when q.put() fails due to Queue Full
        """
        METHOD = "ProcessManager.__init__"

        self._q = Queue(queue_max_size)
        self._queue_timeout = queue_timeout_s
        self._queue_full_max_attempts = queue_full_max_attempts
        self._queue_empty_max_attempts = queue_empty_max_attempts
        self._mermaid_diagram = mermaid_diagram
        log_debug(METHOD,
                  f"queue_max_size: {queue_max_size} queue_timeout_s: {queue_timeout_s} queue_full_max_attempts: {queue_full_max_attempts} queue_empty_max_attempts: {queue_empty_max_attempts}")

    def _enqueue_msg(self, q: Queue, msg_type: str, msg: str, timeout: int):
        METHOD: str = "ProcessManager._enqueue_msg"

        # how many times should we try to enqueue a message if the queue is full?
        WAIT_BEFORE_NEXT_ATTEMPT_S: float = 1
        attempts: int = 0

        while attempts < self._queue_full_max_attempts:
            try:
                attempts += 1
                log_debug(METHOD, f"Trying to enqueue {msg_type} {msg} attempts={attempts}")
                q.put((msg_type, msg), block=True, timeout=timeout)

                if self._mermaid_diagram:
                    proc_id = f"Proc.{os.getpid()}"
                    print(f"    {proc_id} ->> Queue: \"{msg_type} {msg}\"")

                log_debug(METHOD, f"Enqueued {msg_type} {msg}")
                break
            except TimeoutError as e:
                log_debug(METHOD, f"TimeoutError: {e}")
                raise e
            except queue.Full as e:
                log_debug(METHOD, f"queue.Full: {e} attempts: {attempts}")
                if attempts < self._queue_full_max_attempts:
                    log_debug(METHOD, f"Sleeping {WAIT_BEFORE_NEXT_ATTEMPT_S} sec before next attempt")

                    if self._mermaid_diagram:
                        proc_id = f"Proc.{os.getpid()}"
                        print(f"    {proc_id} ->> {proc_id}: queueFull")

                    sleep(WAIT_BEFORE_NEXT_ATTEMPT_S)
                else:
                    raise e
            except Exception as e:
                log_debug(METHOD, f"Exception: {e}")
                raise e

    def _dequeue_msg(self, q: Queue, timeout: int):
        METHOD: str = "ProcessManager._dequeue_msg"

        # how many times should we try to dequeue a message if the queue is empty?
        WAIT_BEFORE_NEXT_ATTEMPT_S: float = 1
        attempts: int = 0

        while attempts < self._queue_empty_max_attempts:
            try:
                attempts += 1
                log_debug(METHOD, "trying to dequeue message")
                msg_type, msg = q.get(block=True, timeout=timeout)

                if self._mermaid_diagram:
                    proc_id = f"Proc.{os.getpid()}"
                    print(f"    Queue ->> {proc_id}: \"{msg_type} {msg}\"")

                log_debug(METHOD, f"dequeued {msg_type} {msg}")
                return msg_type, msg
            except TimeoutError as e:
                log_debug(METHOD, f"TimeoutError: {e}")
                raise e
            except queue.Empty as e:
                log_debug(METHOD, f"queue.Empty: {e} attempts: {attempts}")
                if attempts < self._queue_empty_max_attempts:
                    log_debug(METHOD, f"Sleeping {WAIT_BEFORE_NEXT_ATTEMPT_S} sec before next attempt")

                    if self._mermaid_diagram:
                        proc_id = f"Proc.{os.getpid()}"
                        print(f"    {proc_id} ->> {proc_id}: queueEmpty")

                    sleep(WAIT_BEFORE_NEXT_ATTEMPT_S)
                else:
                    raise e
            except Exception as e:
                log_debug(METHOD, f"Exception: {e}")
                raise e

    def process(self, msg_source: MsgSource, msg_sink: MsgSink, worker_count: int):
        if self._mermaid_diagram:
            print("sequenceDiagram")

        METHOD: str = "ProcessManager.process"

        log_debug(METHOD, "start")

        # create worker pool
        workers = []
        for i in range(worker_count):
            log_debug(METHOD, f"Creating worker process {i}")
            p = Process(target=self._dequeue_and_process_msg, args=(msg_sink,))
            workers.append(p)
            p.start()

        for msg in msg_source.get_msg():
            self._enqueue_msg(self._q, self.MSG_TYPE_USER, msg, self._queue_timeout)

        self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)

        for p in workers:
            log_debug(METHOD, "Joining worker process")
            p.join()

        log_debug(METHOD, "end")

    def _dequeue_and_process_msg(self, msg_sink: MsgSink):
        METHOD: str = "ProcessManager._dequeue_and_process_msg"

        log_debug(METHOD, "start")

        terminate = False

        while not terminate:

            msg_type, msg = self._dequeue_msg(self._q, self._queue_timeout)

            if msg_type:
                if msg_type == self.MSG_TYPE_USER:
                    try:
                        log_debug(METHOD, f"processing {msg_type} {msg}")
                        msg_sink.process_msg(msg)
                    except Exception as e:
                        log_debug(METHOD, f"Error while processing message: {e}")
                elif msg_type == self.MSG_TYPE_QUIT:
                    self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)
                    terminate = True
                else:
                    raise ValueError(f"Unexpected message type {msg_type}")

        log_debug(METHOD, "end")
