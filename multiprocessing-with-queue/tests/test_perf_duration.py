from time import sleep

from src.perf import duration_ns
from src.perf import duration_s


class TestPerfDuration:

    def test_func_duration_nanosecods_no_args(self):
        def wait_zero_point_one_sec():
            sleep(0.1)

        duration, _ = duration_ns(wait_zero_point_one_sec)

        assert type(duration) == int
        assert duration >= 100_000_000  # 100ms in nanoseconds

    def test_func_duration_seconds_no_args(self):
        def wait_zero_point_one_sec():
            sleep(0.1)

        duration, _ = duration_s(wait_zero_point_one_sec)

        assert type(duration) == float
        assert duration >= 0.1

    def test_func_duration_nanosecods_one_arg(self):
        def multiply(a: int):
            return a * 2

        duration, res = duration_ns(multiply, 4)

        assert duration > 0
        assert res == 8
