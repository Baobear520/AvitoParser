from queue import Queue
import time
from datetime import datetime, UTC
import functools
from collections import Counter


def runtime_counter(func):
    """A decorator for measuring and logging a function's runtime."""

    @functools.wraps(func)
    #def wrapper(*args, **kwargs):
        # start_time = time.time()
        
        # # Run the decorated function
        # result = func(*args, **kwargs)

        # # Calculate elapsed time
        # elapsed_time = time.time() - start_time
        # hours, remainder = divmod(elapsed_time, 3600)
        # minutes, seconds = divmod(remainder, 60)
        # milliseconds = (elapsed_time % 1) * 1000
        # # Log the runtime details
        # print(
        #     f"{func.__name__} runtime: "
        #     f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"
        # )
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"Function {func.__name__} executed in {elapsed_time:.2f} seconds.")
        return result
    return wrapper



def get_utc_timestamp():
    curr_dt = datetime.now(UTC)
    timestamp = int(round(curr_dt.timestamp()))
    return timestamp


def dedupe(items, key=None): 
    seen = set()
    for item in items:
        val = item if key is None else key(item)
        if val not in seen:
            yield item 
            seen.add(val)


def return_unique_records(items) -> list:
    records = list(dedupe(items, key=lambda d: d['id']))
    print (f"Returning {len(records)} unique records.")
    return records







