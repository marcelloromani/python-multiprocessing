import logging
from argparse import ArgumentParser
from time import perf_counter_ns

from src.cli_actions import message_factory
from src.log import log_setup


def opt_setup():
    parser = ArgumentParser()

    parser.add_argument(
        "--msg-count",
        type=int,
        required=True,
        help="Number of messages to create."
    )

    parser.add_argument(
        "--queue-size",
        type=int,
        required=True,
        help="Maximum number of messages the queue can hold at any given time."
    )

    parser.add_argument(
        "--queue-timeout",
        type=int,
        required=True,
        help="get() and put() on the queue will wait at most these many seconds before timing out"
    )

    parser.add_argument(
        "--worker-count",
        type=int,
        required=True,
        help="Number of processes reading messages from the queue and processing them"
    )

    parser.add_argument(
        "--task-duration-sec",
        type=float,
        default=2,
        help="How long it takes to process a message (in seconds)."
    )

    parser.add_argument(
        "--queue-full-max-attempts",
        type=int,
        default=1,
        help="Try these many times to put a message in the queue in the face of 'queue Full' errors before raising a queue Full exception."
    )

    parser.add_argument(
        "--queue-empty-max-attempts",
        type=int,
        default=1,
        help="Try these many times to extract a message from the queue in the face of 'queue Empty' errors before raising a queue Empty exception."
    )

    parser.add_argument(
        "--mermaid-diagram",
        action="store_true",
        default=False,
        help="Output a mermaid-compatible sequence diagram."
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "ERROR"],
        default="INFO",
        help="Set log level"
    )

    return parser


def main():
    parser = opt_setup()
    args = parser.parse_args()

    log_setup(logging.getLevelName(args.log_level))

    t_start = perf_counter_ns()
    message_factory(
        num_of_msg_to_create=args.msg_count,
        queue_max_size=args.queue_size,
        queue_get_and_put_timeout_s=args.queue_timeout,
        worker_count=args.worker_count,
        task_duration_sec=args.task_duration_sec,
        queue_full_max_attempts=args.queue_full_max_attempts,
        queue_empty_max_attempts=args.queue_empty_max_attempts,
        mermaid_diagram=args.mermaid_diagram,
        log_level=logging.getLevelName(args.log_level),
    )
    t_end = perf_counter_ns()
    t_elapsed_sec = (t_end - t_start) / 1_000_000_000
    print(f"Elapsed: {t_elapsed_sec}")


if __name__ == "__main__":
    main()
