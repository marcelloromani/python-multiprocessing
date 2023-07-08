import logging
import os
from time import sleep

from src.process_manager import MsgSource, MsgSink
from src.process_manager import ProcessManager


class SimpleMsgSource(MsgSource):
    logger = logging.getLogger("SimpleMsgSource")

    def __init__(self, n: int, task_duration: float):
        """
        :param n: how many messages to produce
        """
        self.logger.debug(f"n={n} task_duration={task_duration}")
        self._n = n
        self._task_duration = task_duration

    def get_msg(self):
        # TODO: rename to get_messages() for more clarity
        self.logger.debug("starting to produce messages")
        for i in range(self._n):
            yield self.create_msg(i)

    def create_msg(self, i: int):
        self.logger.debug(f"Creating message {i}")
        return {
            "duration_s": self._task_duration,
            "msg_id": i,
        }


class SimpleMsgSink(MsgSink):
    logger = logging.getLogger('SimpleMsgSink')

    def __init__(self, mermaid_diagram: bool = False):
        self.logger.debug("Constructor")
        self._processed_message = 0
        self._mermaid_diagram = mermaid_diagram

    def __del__(self):
        self.logger.debug(f"Destructor: Processed {self._processed_message} messages")

    def process_msg(self, msg):
        self.logger.debug("Start")

        if self._mermaid_diagram:
            proc_id = f"Proc.{os.getpid()}"
            print(f"    {proc_id} ->> {proc_id}: \"process_msg({msg})\"")

        self.logger.debug(f"Processing {msg}")
        duration_s = msg["duration_s"]
        sleep(duration_s)
        self._processed_message += 1

        self.logger.debug("End")


def message_factory(num_of_msg_to_create: int, queue_max_size: int, queue_get_and_put_timeout_s: int,
                    worker_count: int, task_duration_sec: float, queue_full_max_attempts: int,
                    queue_empty_max_attempts: int, mermaid_diagram: bool, log_level: int):
    """
    Creates and run the whole "message producer / queue / consumers" setup based on the provided parameters
    :param num_of_msg_to_create: the producer will create and (try to) enqueue this many messages before terminating
    :param queue_max_size: maximum number of messages the queue can hold at any given time
    :param queue_get_and_put_timeout_s: q.put() and q.get() operations will wait at most these many seconds before timing out
    :param worker_count: number of processes reading messages off the queue and executing/processing them
    :param task_duration_sec: how long will it take to process a message (to simulate io-bound msg processing)
    :param queue_full_max_attempts: Try these many times to put a message in the queue in the face o 'queue Full' errors before raising a queue Full exception.
    :param queue_empty_max_attempts: Try these many times to extract a message from the queue in the face o 'queue Empty' errors before raising a queue Empty exception
    :param mermaid_diagram: if True, produce a Mermaid-compatible sequence diagram.
    :param log_level: logging-compatible log level
    """
    src = SimpleMsgSource(num_of_msg_to_create, task_duration_sec)
    dst = SimpleMsgSink(mermaid_diagram)
    p = ProcessManager(queue_max_size, queue_get_and_put_timeout_s, queue_full_max_attempts, queue_empty_max_attempts,
                       mermaid_diagram, log_level)
    p.process(src, dst, worker_count)
