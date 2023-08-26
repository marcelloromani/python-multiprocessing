from time import perf_counter_ns


def duration_ns(func) -> int:
    t_start = perf_counter_ns()
    func()
    t_end = perf_counter_ns()
    return t_end - t_start


def duration_s(func) -> float:
    nanos = duration_ns(func)
    return nanos / 1_000_000_000
