import os
from datetime import datetime

LOG_ENABLED = True


def log_debug(f: str, msg: str):
    if not LOG_ENABLED:
        return
    t = datetime.now()
    pid = os.getpid()
    print(f"{t} [DEBUG] [{pid}] {f} {msg}")
