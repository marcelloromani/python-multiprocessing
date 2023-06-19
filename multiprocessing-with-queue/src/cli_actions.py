from time import sleep

from src.log import log_debug
from src.process_manager import MsgSource, MsgSink
from src.process_manager import ProcessManager


class SimpleMsgSource(MsgSource):
    def __init__(self, n: int, task_duration: float):
        """
        :param n: how many messages to produce
        """
        log_debug("SimpleMsgSource.__init__", f"n={n} task_duration={task_duration}")
        self._n = n
        self._task_duration = task_duration

    def get_msg(self):
        log_debug("SimpleMsgSource.get_msg", "")
        for i in range(self._n):
            yield self.create_msg(i)

    def create_msg(self, i: int):
        log_debug("SimpleMsgSource.create_msg", str(i))
        return {
            "duration_s": self._task_duration,
            "msg_id": i,
        }


class SimpleMsgSink(MsgSink):

    def __init__(self):
        METHOD = "SimpleMsgSink.__init__"
        log_debug(METHOD, "")
        self._processed_message = 0

    def __del__(self):
        METHOD = "SimpleMsgSink.__del__"
        log_debug(METHOD, f"Processed {self._processed_message} messages.")

    def process_msg(self, msg):
        METHOD = "SimpleMsgSink.process_msg"
        log_debug(METHOD, "start")
        duration_s = msg["duration_s"]
        log_debug(METHOD, f"Processing {msg}")
        sleep(duration_s)
        self._processed_message += 1
        log_debug(METHOD, "end")


def message_factory(num_of_msg_to_create: int, queue_max_size: int, queue_get_and_put_timeout_s: int,
                    worker_count: int, task_duration_sec: float, queue_full_max_attempts: int):
    """
    Creates and run the whole "message producer / queue / consumers" setup based on the provided parameters
    :param num_of_msg_to_create: the producer will create and (try to) enqueue this many messages before terminating
    :param queue_max_size: maximum number of messages the queue can hold at any given time
    :param queue_get_and_put_timeout_s: q.put() and q.get() operations will wait at most these many seconds before timing out
    :param worker_count: number of processes reading messages off the queue and executing/processing them
    :param task_duration_sec: how long will it take to process a message (to simulate io-bound msg processing)
    :param queue_full_max_attempts: "Try these many times to put a message in the queue in the face o 'queue Full' errors before raising a queue Full exception.
    """
    src = SimpleMsgSource(num_of_msg_to_create, task_duration_sec)
    dst = SimpleMsgSink()
    p = ProcessManager(queue_max_size, queue_get_and_put_timeout_s, queue_full_max_attempts)
    p.process(src, dst, worker_count)
