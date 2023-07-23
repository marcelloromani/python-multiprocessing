"""Opinionated logging configuration"""

import logging
import sys


def log_setup(log_level):
    """
    Configure log formatter and set log level.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    log_handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(module)s(%(process)d) - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)
