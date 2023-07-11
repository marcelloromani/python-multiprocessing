"""Implementation of actions routed from CLI options in main.py"""

import logging
import os
from time import sleep

from src.process_manager import MsgSource, MsgSink
from src.process_manager import ProcessManager


class SimpleMsgSource(MsgSource):
    """
    Creates simple messages containing a timeout indication
    to simulate real workloads taking some time
    """
    logger = logging.getLogger("SimpleMsgSource")

    def __init__(self, msg_count: int, task_duration_s: float):
        """
        :param msg_count: how many messages to produce
        :param task_duration_s: dummy task duration (seconds)
        """
        self.logger.debug("n=%d task_duration=%d", msg_count, task_duration_s)
        self._msg_count = msg_count
        self._task_duration_s = task_duration_s

    def get_msg(self):
        # TODO: rename to get_messages() for more clarity
        self.logger.debug("starting to produce messages")
        for i in range(self._msg_count):
            yield self.create_msg(i)

    def create_msg(self, i: int):
        """
        Create a single message
        :param i: message index
        :return: dict
        """
        self.logger.debug("Creating message %d", i)
        return {
            "duration_s": self._task_duration_s,
            "msg_id": i,
        }


class SimpleMsgSink(MsgSink):
    """
    Consume messages by SimpleMsgSource by waiting for the number of seconds specified in the message body.
    """
    logger = logging.getLogger('SimpleMsgSink')

    def __init__(self, mermaid_diagram: bool = False):
        """
        :param mermaid_diagram: if True, print directives to create a Mermaid sequence diagram
        """
        self.logger.debug("Constructor")
        self._processed_message_count = 0
        self._mermaid_diagram = mermaid_diagram

    def __del__(self):
        """Log object deletion"""
        self.logger.debug("Destructor: Processed %d messages", self._processed_message_count)

    def process_msg(self, msg):
        """
        Process the specified message.
        """
        self.logger.debug("Start")

        if self._mermaid_diagram:
            proc_id = f"Proc.{os.getpid()}"
            print(f"    {proc_id} ->> {proc_id}: \"process_msg({msg})\"")

        self.logger.debug("Processing %d", msg)
        duration_s = msg["duration_s"]
        sleep(duration_s)
        self._processed_message_count += 1

        self.logger.debug("End")


def message_factory(num_of_msg_to_create: int, queue_max_size: int, queue_get_and_put_timeout_s: int,
                    worker_count: int, task_duration_sec: float, queue_full_max_attempts: int,
                    queue_empty_max_attempts: int, mermaid_diagram: bool, log_level: int):
    """
    Creates and run the whole "message producer / queue / consumers" setup based on the provided parameters
    :param num_of_msg_to_create: the producer will create and (try to) enqueue this many messages before terminating
    :param queue_max_size: maximum number of messages the queue can hold at any given time
    :param queue_get_and_put_timeout_s: q.put() and q.get() will wait at most these many seconds before timing out
    :param worker_count: number of processes reading messages off the queue and executing/processing them
    :param task_duration_sec: how long will it take to process a message (to simulate io-bound msg processing)
    :param queue_full_max_attempts: Retry when queue is full before raising queue full exception
    :param queue_empty_max_attempts: Retry when queue is empty before raising queue empty exception
    :param mermaid_diagram: if True, produce a Mermaid-compatible sequence diagram.
    :param log_level: logging-compatible log level
    """
    src = SimpleMsgSource(num_of_msg_to_create, task_duration_sec)
    dst = SimpleMsgSink(mermaid_diagram)
    proc_mgr = ProcessManager(
        queue_max_size,
        queue_get_and_put_timeout_s,
        queue_full_max_attempts,
        queue_empty_max_attempts,
        mermaid_diagram,
        log_level
    )
    proc_mgr.process(src, dst, worker_count)
