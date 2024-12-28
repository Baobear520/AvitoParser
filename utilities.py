import time
import datetime
import functools


def runtime_counter(func):
    """A decorator for measuring and logging a function's runtime."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            # Run the decorated function
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            return None  # Explicitly return None if an error occurs
        finally:
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            hours, remainder = divmod(elapsed_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            milliseconds = (elapsed_time % 1) * 1000
            # Log the runtime details
            print(
                f"{func.__name__} runtime: "
                f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"
            )
    return wrapper


def get_timestamp(current_time):
    return datetime.datetime.timestamp(current_time)*1000

