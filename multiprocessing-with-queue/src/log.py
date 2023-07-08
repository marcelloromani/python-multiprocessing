import logging
import sys


def log_setup(log_level):
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(name)s(%(process)d).%(funcName)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)
