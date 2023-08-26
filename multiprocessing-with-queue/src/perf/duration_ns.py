from time import perf_counter_ns


def duration_ns(func, *args, **kwargs):
    t_start = perf_counter_ns()
    res = func(*args, **kwargs)
    t_end = perf_counter_ns()
    return t_end - t_start, res


def duration_s(func, *args, **kwargs):
    nanos, res = duration_ns(func, *args, **kwargs)
    return nanos / 1_000_000_000, res
