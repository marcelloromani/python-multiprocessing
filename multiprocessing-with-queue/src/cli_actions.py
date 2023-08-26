"""Implementation of actions routed from CLI options in main.py"""

import logging
from time import perf_counter_ns
from time import sleep

from src.config import Config
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

    def yield_msgs(self):
        self.logger.debug("start: produce messages")
        for i in range(self._msg_count):
            yield self.create_msg(i)
        self.logger.debug("end: produce messages")

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
    logger = logging.getLogger("SimpleMsgConsumer")

    def __init__(self):
        self.logger.debug("Constructor")
        self._processed_message_count = 0

    @property
    def processed_message_count(self):
        return self._processed_message_count

    def __del__(self):
        """Log object deletion"""
        self.logger.debug("Destructor: Processed %d messages", self._processed_message_count)

    def process_msg(self, msg):
        """
        Process the specified message.
        """
        self.logger.debug("Processing %s", msg)
        duration_s = msg["duration_s"]
        sleep(duration_s)
        self._processed_message_count += 1


def run_session(config: Config, consumer_min, consumer_max, consumer_step):
    logger = logging.getLogger("RunSession")

    print(config.csv_headers())
    for consumer_count in range(consumer_min, consumer_max + 1, consumer_step):
        config["consumer_count"] = consumer_count
        t_start = perf_counter_ns()
        run_single(config)
        t_end = perf_counter_ns()
        t_elapsed_sec = (t_end - t_start) / 1_000_000_000

        print(config.csv_row(t_elapsed_sec))


def run_single(config: Config):
    producer = SimpleMsgProducer(config.msg_count, config.task_duration_sec)
    consumer = SimpleMsgConsumer()

    enqueuer = MsgEnqueuer(config.queue_put_timeout_sec, config.queue_full_max_attempts, config.queue_full_wait_sec)
    dequeuer = MsgDequeuer(config.queue_get_timeout_sec, config.queue_empty_max_attempts, config.queue_empty_wait_sec)

    proc_mgr = ProcessManager(enqueuer, dequeuer, config.queue_max_size)
    proc_mgr.process(producer, consumer, config.consumer_count)
