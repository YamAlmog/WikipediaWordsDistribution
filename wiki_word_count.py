import pandas as pd
import requests
import re
import logging
from error import PageNotFoundException, RateLimitException
import threading
import  concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time
import warnings
warnings.filterwarnings('ignore')
import random

logging.basicConfig(filename='app.log', filemode='w',  level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
topic_file_path = "topic.txt"
lock = threading.Lock()
max_retries = 3

# Decorator function for estimate the main function execution time
def tracer(func):
    def wrapper(*args):
        logging.debug(f"{func.__name__} function")
        before = time.time()
        result  = func(*args)
        logging.debug(f"Execution time: {time.time() - before} seconds")
        return result
    return wrapper

# Decorator function for get_wikipedia_page function
def retry_on_rate_limit_exception(func):
    def wrapper(*args):
        for attempt in range(max_retries): 
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

# get topics file path, return topics list
# possible errors: FileNotFoundError
def read_file(file_path) -> list:
    topic_list = []
    try:    
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                # use strip() to remove newline characters
                topic = line.strip()
                topic_list.append(topic.lower())

        return topic_list
    except FileNotFoundError:
        print(f"The file '{file_path}' does not exist.")
    except Exception as e:
        print(f"An error occurred while reading the topic file: {e}")


# get page title, return page content 
# possible errors: RateLimitException and page not found or page content=None
@retry_on_rate_limit_exception
def get_wikipedia_page(title: str) -> str:
    base_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "titles": title,
        "prop": "revisions",
        "rvprop": "content",
    }    
    response = requests.get(base_url, params=params)
    if response.status_code == 429:
        raise RateLimitException()
        
    data = response.json()
    if data == None:
        raise RateLimitException()

    # Check if 'query' key exists in the response
    if 'query' not in data:
        print("Error: 'query' key not found in API response.")
        return None
    # Extract page content from the API response
    page_id = next(iter(data["query"]["pages"]))
    if page_id == "-1":
        # Page not found
        return None

    page_content = data["query"]["pages"][page_id]["revisions"][0]["*"]
    return page_content

    
# get topic, return word count dictionary
# possible errors: HTTPError, ConnectionError, RequestException while call get_wikipedia_page function
def words_distribution_to_dict(topic: str) -> dict:
    words_count_dict = {}
    try:
        page_content = get_wikipedia_page(topic)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while try to get {topic} page: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while try to get {topic} page: {e}")     
    except requests.exceptions.RequestException as e:
        print(f"An unexpected error occurred while try to get {topic} page: {e}")

    if page_content == None:
        raise PageNotFoundException(f"Page: {topic} not found")
    # clean punctuation marks and non-letter characters
    else:    
        words = re.findall(r'\b[a-zA-Z]+\b', page_content)
        for word in words:
            if word.lower() not in words_count_dict:    
                words_count_dict[word.lower()] = 1
            else:
                words_count_dict[word.lower()] += 1

        return words_count_dict

# get df and topic, update the df using the word count dictionary
# possible errors: page topic not found
def word_distribution_task(df: pd.DataFrame, topic: str):
    try:    
        words_count_dict = words_distribution_to_dict(topic)
        logging.debug(f"create word dict for topic: {topic}")
    except PageNotFoundException as e:
        print(e)
        return
    lock.acquire()
    for word in words_count_dict.keys():
        if word not in df.columns:
            df[word] = None
        df.at[topic, word] = words_count_dict[word]
    lock.release() 

@tracer
def main():
    global  words_count_dict
    topic_list = read_file(topic_file_path)
    logging.debug(topic_list)
    futures = []
    max_threads = 3
    shared_data_frame = pd.DataFrame(index=topic_list)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            # Submit tasks to the pool
            for topic in topic_list:    
                future = executor.submit(word_distribution_task, shared_data_frame, topic)
                logging.debug("future has created")
                futures.append(future)
                # Wait for all tasks to complete
            concurrent.futures.wait(futures)
            shared_data_frame.to_csv('word_distribution.csv', index=True)
            print("Update word_distribution csv file")
    except Exception as e:
        print(f"Unknown Error: {e}")


if __name__ == "__main__":
    main()
