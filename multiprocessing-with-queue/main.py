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


def main():
    src = SimpleMsgSource(10)
    dst = SimpleMsgSink()
    p = ProcessManager(queue_max_size=1, queue_timeout_s=1)
    p.process(src, dst, worker_count=5)


if __name__ == "__main__":
    main()
