import logging

from box import Box


class Config(Box):
    logger = logging.getLogger("Config")

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

    @classmethod
    def from_argparser_args(cls, args):
        obj = Config()
        for item in cls.CONFIG_ITEMS:
            obj[item] = eval(f"args.{item}")
        return obj

    def log_values(self):
        for key in self.CONFIG_ITEMS:
            self.logger.info("%s = %s", key, self[key])

    def csv_headers(self) -> str:
        csv_headers = self.CONFIG_ITEMS.copy()
        csv_headers.insert(0, "run_id")
        csv_headers.append("elapsed")
        return ",".join(csv_headers)

    def csv_row(self, elapsed_sec: float):
        csv_row = [self[item] for item in self.CONFIG_ITEMS]
        csv_row.insert(0, 1)
        csv_row.append(elapsed_sec)
        csv_row_str = [str(item) for item in csv_row]
        return ",".join(csv_row_str)
