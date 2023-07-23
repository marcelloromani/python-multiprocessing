"""Implementation of actions routed from CLI options in main.py"""

import logging
from time import sleep

from src.process_manager import MsgEnqueuer, MsgDequeuer
from src.process_manager import MsgProducer, MsgConsumer
from src.process_manager import ProcessManager


class SimpleMsgProducer(MsgProducer):
    """
    Creates simple messages containing a timeout indication
    to simulate real workloads taking some time
    """
    logger = logging.getLogger("SimpleMsgProducer")

    def __init__(self, msg_count: int, task_duration_s: float):
        """
        :param msg_count: how many messages to produce
        :param task_duration_s: dummy task duration (seconds)
        """
        self.logger.debug("n=%d task_duration=%d", msg_count, task_duration_s)
        self._msg_count = msg_count
        self._task_duration_s = task_duration_s

    def yield_msg(self):
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


class SimpleMsgConsumer(MsgConsumer):
    """
    Consume messages by SimpleMsgSource by waiting for the number of seconds specified in the message body.
    """
    logger = logging.getLogger('SimpleMsgConsumer')

    def __init__(self):
        self.logger.debug("Constructor")
        self._processed_message_count = 0

    def __del__(self):
        """Log object deletion"""
        self.logger.debug("Destructor: Processed %d messages", self._processed_message_count)

    def process_msg(self, msg):
        """
        Process the specified message.
        """
        self.logger.debug("Start")

        self.logger.debug("Processing %s", msg)
        duration_s = msg["duration_s"]
        sleep(duration_s)
        self._processed_message_count += 1

        self.logger.debug("End")


def message_factory(
        num_of_msg_to_create: int,
        task_duration_sec: float,
        queue_max_size: int,
        consumer_count: int,
        queue_put_timeout_s: float,
        queue_full_max_attempts: int,
        queue_full_wait_s: float,
        queue_get_timeout_s: int,
        queue_empty_max_attempts: int,
        queue_empty_wait_s: float,
):
    """
    Creates and run the whole "message producer / queue / consumers" setup based on the provided parameters
    :param num_of_msg_to_create: the producer will create and (try to) enqueue this many messages before terminating
    :param task_duration_sec: how long will it take to process a message (to simulate io-bound msg processing)
    :param queue_max_size: maximum number of messages the queue can hold at any given time
    :param consumer_count: number of processes reading messages off the queue and executing/processing them
    :param queue_put_timeout_s: q.put() will wait this much before timing out
    :param queue_full_max_attempts: Retry when queue is full before raising queue full exception
    :param queue_full_wait_s: when queue is full, wait this much before retrying
    :param queue_get_timeout_s: q.get() will wait this much before timing out
    :param queue_empty_max_attempts: Retry when queue is empty before raising queue empty exception
    :param queue_empty_wait_s: when queue is empty, wait this much before retrying
    """
    producer = SimpleMsgProducer(num_of_msg_to_create, task_duration_sec)
    consumer = SimpleMsgConsumer()

    enqueuer = MsgEnqueuer(queue_put_timeout_s, queue_full_max_attempts, queue_full_wait_s)
    dequeuer = MsgDequeuer(queue_get_timeout_s, queue_empty_max_attempts, queue_empty_wait_s)

    proc_mgr = ProcessManager(enqueuer, dequeuer, queue_max_size)
    proc_mgr.process(producer, consumer, consumer_count)
