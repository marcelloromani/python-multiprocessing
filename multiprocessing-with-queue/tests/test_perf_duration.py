from time import sleep

from src.perf import duration_ns
from src.perf import duration_s


class TestPerfDuration:

    def test_func_duration_nanosecods_no_args(self):
        def wait_zero_point_one_sec():
            sleep(0.1)

        duration = duration_ns(wait_zero_point_one_sec)

        assert type(duration) == int
        assert duration >= 100_000_000  # 100ms in nanoseconds

    def test_func_duration_secods_no_args(self):
        def wait_zero_point_one_sec():
            sleep(0.1)

        duration = duration_s(wait_zero_point_one_sec)

        assert type(duration) == float
        assert duration >= 0.1
