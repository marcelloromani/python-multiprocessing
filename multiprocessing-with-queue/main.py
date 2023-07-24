"""Play with Python multiprocessing module"""

import logging
from argparse import ArgumentParser
from time import perf_counter_ns

from src.cli_actions import message_factory
from src.config import Config
from src.log import log_setup


def opt_setup():
    """Configure command-line options"""

    parser = ArgumentParser()

    parser.add_argument(
        "--msg-count",
        type=int,
        default=5,
        help="Number of messages to create."
    )

    parser.add_argument(
        "--queue-max-size",
        type=int,
        default=1,
        help="Maximum number of messages the queue can hold at any given time."
    )

    parser.add_argument(
        "--queue-put-timeout-sec",
        type=int,
        default=1,
        help="put() on the queue will wait at most these many seconds before timing out"
    )

    parser.add_argument(
        "--queue-get-timeout-sec",
        type=int,
        default=1,
        help="get() on the queue will wait at most these many seconds before timing out"
    )

    parser.add_argument(
        "--consumer-count",
        type=int,
        default=1,
        help="Number of processes reading messages from the queue and processing them"
    )

    parser.add_argument(
        "--task-duration-sec",
        type=float,
        default=0.5,
        help="How long it takes to process a message (in seconds)."
    )

    parser.add_argument(
        "--queue-full-max-attempts",
        type=int,
        default=1,
        help="Retry when queue is full before raising queue full exception"
    )

    parser.add_argument(
        "--queue-full-wait-sec",
        type=int,
        default=1,
        help="when queue is full, wait this much before retrying."
    )

    parser.add_argument(
        "--queue-empty-wait-sec",
        type=int,
        default=1,
        help="when queue is empty, wait this much before retrying."
    )

    parser.add_argument(
        "--queue-empty-max-attempts",
        type=int,
        default=1,
        help="Retry when queue is empty before raising queue empty exception"
    )

    # parser.add_argument(
    #     "--mermaid-diagram",
    #     action="store_true",
    #     default=False,
    #     help="Output a mermaid-compatible sequence diagram."
    # )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "ERROR"],
        default="INFO",
        help="Set log level"
    )

    return parser


def main():
    logger = logging.getLogger("main")

    # Instantiate argument parser
    parser = opt_setup()

    # Get command line arguments
    args = parser.parse_args()

    # Configure logging library
    log_setup(logging.getLevelName(args.log_level))

    config = Config.from_argparser_args(args)
    config.log_values()

    t_start = perf_counter_ns()
    message_factory(config)
    t_end = perf_counter_ns()
    t_elapsed_sec = (t_end - t_start) / 1_000_000_000
    logger.info("Elapsed: %s", t_elapsed_sec)


if __name__ == "__main__":
    main()
