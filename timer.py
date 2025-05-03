
import time

def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter() # Start the timer
        result = func(*args, **kwargs)
        end_time = time.perf_counter()    # End the timer
        print(f"{func.__name__} took {end_time - start_time:.4f} seconds")
        return result
    return wrapper
