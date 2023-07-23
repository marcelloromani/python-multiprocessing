import logging

from box import Box


class Config(Box):
    logger = logging.getLogger()

    CONFIG_ITEMS = [
        "msg_count",
        "task_duration_sec",
        "queue_max_size",
        "consumer_count",
        "queue_put_timeout_sec",
        "queue_full_max_attempts",
        "queue_full_wait_sec",
        "queue_get_timeout_sec",
        "queue_empty_max_attempts",
        "queue_empty_wait_sec",
    ]

    def log_values(self):
        for item in self.CONFIG_ITEMS:
            self.logger.info("%s = %s", item, self[item])

    @classmethod
    def from_argparser_args(cls, args):
        obj = Config()
        for item in cls.CONFIG_ITEMS:
            obj[item] = eval(f"args.{item}")
        return obj