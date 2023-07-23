"""
Connects a message source and a number of message sinks through a queue.
"""
import logging
from multiprocessing import Queue, Process

from src.log import log_setup
from .interfaces import MsgProducer, MsgConsumer
from .msg_dequeuer import MsgDequeuer
from .msg_enqueuer import MsgEnqueuer


class ProcessManager:
    """
    Connects a message source and a number of message sinks through a queue.
    """

    MSG_TYPE_USER: str = "USER"
    MSG_TYPE_QUIT: str = "QUIT"

    logger = logging.getLogger()

    def __init__(self, enqueuer: MsgEnqueuer, dequeuer: MsgDequeuer, queue_max_size: int = 2):
        self._q = Queue(queue_max_size)
        self._enqueuer = enqueuer
        self._dequeuer = dequeuer
        self._log_level = self.logger.getEffectiveLevel()

    def process(self, producer: MsgProducer, consumer: MsgConsumer, consumer_count: int):
        """
        :param producer: single source of messages
        :param consumer: processes one message at a time
        :param consumer_count: number of consumer processes to instantiate
        """
        # create worker pool
        workers = []
        for worker_index in range(consumer_count):
            self.logger.debug("Creating worker process %d", worker_index)
            worker_process = Process(target=self._dequeue_and_process_msg, args=(consumer,))
            workers.append(worker_process)
            worker_process.start()

        # put all messages from the producer on the queue
        for msg in producer.yield_msg():
            self._enqueuer.put(self._q, self.MSG_TYPE_USER, msg)

        # lastly, put the QUIT message on the queue to signal no more user messages
        self._enqueuer.put(self._q, self.MSG_TYPE_QUIT, "")

        for worker_process in workers:
            self.logger.debug("Joining worker process %d", worker_process.pid)
            worker_process.join()

        self.logger.debug("end")

    def _dequeue_and_process_msg(self, consumer: MsgConsumer):
        # we're on a new process, sys.stdout is different from our parent process
        log_setup(self._log_level)
        self.logger = logging.getLogger()

        self.logger.debug("start")

        terminate = False

        while not terminate:

            msg_type, msg = self._dequeuer.get(self._q)

            if msg_type is None:
                continue

            if msg_type == self.MSG_TYPE_USER:
                self.logger.debug("processing %s %s", msg_type, msg)
                consumer.process_msg(msg)
            elif msg_type == self.MSG_TYPE_QUIT:
                self.logger.debug("Enqueueing QUIT message")
                self._enqueuer.put(self._q, self.MSG_TYPE_QUIT, "")
                terminate = True
            else:
                raise ValueError(f"Unexpected message type {msg_type}")

        self.logger.debug("end")
