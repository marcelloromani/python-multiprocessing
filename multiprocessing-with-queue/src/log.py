"""Opinionated logging configuration"""

import logging
import sys


def log_setup(log_level):
    """
    Configure log formatter and set log level.
    """
    root_logger = logging.getLogger()

    if type(log_level) == str:
        log_level = log_level.upper()

    root_logger.setLevel(log_level)

    log_handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(name)s(%(process)d) - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)
