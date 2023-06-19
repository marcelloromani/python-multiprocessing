import queue
from multiprocessing import Queue, Process

from src.log import log_info


class MsgSource:

    def get_msg(self):
        raise NotImplementedError()


class MsgSink:

    def process_msg(self, msg):
        raise NotImplementedError()


class ProcessManager:
    MSG_TYPE_USER: str = "USER"
    MSG_TYPE_QUIT: str = "QUIT"

    def __init__(self, queue_max_size: int, queue_timeout_s: int = 10):
        """
        :param queue_max_size: maximum size of the internal queue
        :param queue_timeout_s: how long to wait for a q.get() or q.put() to succeed before declaring it failed
        """
        self._q = Queue(queue_max_size)
        self._queue_timeout = queue_timeout_s
        log_info("ProcessManager", f"queue_max_size: {queue_max_size} queue_timeout_s: {queue_timeout_s}")

    @staticmethod
    def _enqueue_msg(q: Queue, msg_type: str, msg: str, timeout: int):
        try:
            log_info("_enqueue_msg", f"Trying to enqueue {msg_type} {msg}")
            q.put((msg_type, msg), block=True, timeout=timeout)
            log_info("_enqueue_msg", f"Enqueued {msg_type} {msg}")
        except TimeoutError as e:
            log_info("_enqueue_msg", f"TimeoutError: {e}")
            raise e
        except queue.Full as e:
            log_info("_enqueue_msg", f"queue.Full: {e}")
            raise e
        except Exception as e:
            log_info("_enqueue_msg", f"Exception: {e}")
            raise e

    @staticmethod
    def _dequeue_msg(q: Queue, timeout: int):
        try:
            log_info("_dequeue_msg", "trying to dequeue message")
            msg_type, msg = q.get(block=True, timeout=timeout)
            log_info("_dequeue_msg", f"dequeued {msg_type} {msg}")
            return msg_type, msg
        except TimeoutError as e:
            log_info("_dequeue_msg", f"TimeoutError: {e}")
            raise e
        except queue.Empty as e:
            log_info("_dequeue_msg", f"queue.Empty: {e}")
            raise e

    def process(self, msg_source: MsgSource, msg_sink: MsgSink, worker_count: int):
        log_info("process", "start")

        # create worker pool
        workers = []
        for i in range(worker_count):
            log_info("process", f"Creating worker process {i}")
            p = Process(target=self._dequeue_and_process_msg, args=(msg_sink,))
            workers.append(p)
            p.start()

        for msg in msg_source.get_msg():
            self._enqueue_msg(self._q, self.MSG_TYPE_USER, msg, self._queue_timeout)

        self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)

        for p in workers:
            log_info("process", "Joining worker process")
            p.join()

        log_info("process", "end")

    def _dequeue_and_process_msg(self, msg_sink: MsgSink):
        log_info("_dequeue_and_process_msg", "start")

        terminate = False

        while not terminate:

            msg_type, msg = self._dequeue_msg(self._q, self._queue_timeout)

            if msg_type:
                if msg_type == self.MSG_TYPE_USER:
                    try:
                        log_info("_dequeue_and_process_msg", f"processing {msg_type} {msg}")
                        msg_sink.process_msg(msg)
                    except Exception as e:
                        log_info("_dequeue_and_process_msg", f"Error while processing message: {e}")
                elif msg_type == self.MSG_TYPE_QUIT:
                    self._enqueue_msg(self._q, self.MSG_TYPE_QUIT, "", self._queue_timeout)
                    terminate = True
                else:
                    raise ValueError(f"Unexpected message type {msg_type}")

        log_info("_dequeue_and_process_msg", "end")
