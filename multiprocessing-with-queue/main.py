from time import sleep

from src.log import log_info
from src.process_manager import MsgSource, MsgSink
from src.process_manager import ProcessManager


class SimpleMsgSource(MsgSource):
    def __init__(self, n: int):
        """
        :param n: how many messages to produce
        """
        self._n = n

    def get_msg(self):
        for i in range(self._n):
            yield self.create_msg(i)

    def create_msg(self, i: int):
        return {
            "duration_s": 2,
            "msg_id": i,
        }


class SimpleMsgSink(MsgSink):
    def process_msg(self, msg):
        log_info("process_msg", "start")
        duration_s = msg["duration_s"]
        log_info("process_msg", f"Processing {msg}")
        sleep(duration_s)
        log_info("process_msg", "end")


def message_factory(num_of_msg_to_create: int, queue_max_size: int, queue_get_and_put_timeout_s: int, worker_count: int):
    """
    Creates and run the whole "message producer / queue / consumers" setup based on the provided parameters
    :param num_of_msg_to_create: the producer will create and (try to) enqueue this many messages before terminating
    :param queue_max_size: maximum number of messages the queue can hold at any given time
    :param queue_get_and_put_timeout_s: q.put() and q.get() operations will wait at most these many seconds before timing out
    :param worker_count: number of processes reading messages off the queue and executing/processing them
    """
    src = SimpleMsgSource(num_of_msg_to_create)
    dst = SimpleMsgSink()
    p = ProcessManager(queue_max_size, queue_get_and_put_timeout_s)
    p.process(src, dst, worker_count)


def main():
    message_factory(5, 1, 1, 10)


if __name__ == "__main__":
    main()
