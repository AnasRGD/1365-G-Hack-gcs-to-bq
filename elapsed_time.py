import functools
import time

import gcp_logging


def log_elapsed_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        gcp_logging.info(
            f'{func.__name__} took {time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}'
        )
        return result

    return wrapper