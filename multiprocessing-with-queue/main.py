from argparse import ArgumentParser
from time import perf_counter_ns

from src.cli_actions import message_factory


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
        help="number of processes reading messages from the queue and processing them"
    )

    return parser


def main():
    parser = opt_setup()
    args = parser.parse_args()

    t_start = perf_counter_ns()
    message_factory(
        num_of_msg_to_create=args.msg_count,
        queue_max_size=args.queue_size,
        queue_get_and_put_timeout_s=args.queue_timeout,
        worker_count=args.worker_count
    )
    t_end = perf_counter_ns()
    t_elapsed_sec = (t_end - t_start) / 1_000_000_000
    print(f"Elapsed: {t_elapsed_sec}")


if __name__ == "__main__":
    main()
