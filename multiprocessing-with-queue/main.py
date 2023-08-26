#!/usr/bin/env python
"""Play with Python multiprocessing module"""

import logging
from argparse import ArgumentParser

from src.cli_actions import run_session
from src.cli_actions import run_single
from src.config import Config
from src.log import log_setup
from src.perf import duration_s


def opt_setup():
    """Configure command-line options"""

    parser = ArgumentParser()

    parser.add_argument(
        "--perftest-consumer-count",
        type=int,
        nargs=3,
        metavar=('min', 'max', 'step'),
        help="Perform multiple runs with an increasing number of consumer processes. Print elapsed time in CSV format for easy graphing."
    )

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
        type=float,
        default=1,
        help="when queue is full, wait this much before retrying."
    )

    parser.add_argument(
        "--queue-empty-wait-sec",
        type=float,
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

    # TODO see if there's a way to list all options provided to args and log their values

    if args.perftest_consumer_count is None:
        # single run with the specified number of consumer processes
        config = Config.from_argparser_args(args)
        config.log_values()
        elapsed, _ = duration_s(run_single, config)
        logger.info("Elapsed: %f", elapsed)
    else:
        # multiple run with an increasing number of consumer processes, CSV output
        config = Config.from_argparser_args(args)
        config.log_values()
        consumer_min, consumer_max, consumer_step = args.perftest_consumer_count
        run_session(config, consumer_min, consumer_max, consumer_step)


if __name__ == "__main__":
    main()
