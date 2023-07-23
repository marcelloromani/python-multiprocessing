import pytest

from main import main


class TestMain:

    def test_should_run_with_no_args(self):
        with pytest.raises(SystemExit):
            main()
