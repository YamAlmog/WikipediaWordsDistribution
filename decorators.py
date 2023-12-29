
import time
import logging
from error import RateLimitException
import random

MAX_RETRIES = 3


# Decorator function for estimate the main function execution time
def tracer(func):
    def wrapper(*args):
        print(f"{func.__name__} function")
        before = time.time()
        result  = func(*args)
        print(f"Execution time: {time.time() - before} seconds")
        return result
    return wrapper


# Decorator function for get_wikipedia_page function
def retry_on_rate_limit_exception(func):
    def wrapper(*args):
        for attempt in range(MAX_RETRIES): 
            try:
                result = func(*args)
                return result
            except RateLimitException:
                logging.debug('rate limit handling')
                retry_delay = random.randint(1,7)
                logging.warning(f"Rate limit exceeded. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
        logging.error(f"Could not retrieve page {args[0]}")
    
    return wrapper