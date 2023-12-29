import pandas as pd
import requests
import re
import logging
from error import PageNotFoundException, RateLimitException
from concurrent.futures import ThreadPoolExecutor
import time
import warnings
import random
import asyncio
from decorators import retry_on_rate_limit_exception, tracer


warnings.filterwarnings('ignore')
logging.basicConfig(filemode='w',  level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


TOPIC_FILE_PATH = "topic.txt"


# get topics file path, return topics list
# possible errors: FileNotFoundError
def read_topic_file(file_path) -> list:
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
async def word_distribution_task(df: pd.DataFrame, topic: str) -> None:
    try:    
        words_count_dict = words_distribution_to_dict(topic)
        logging.debug(f"create word dict for topic: {topic}")
    except PageNotFoundException as e:
        print(e)
        return
    
    for word in words_count_dict.keys():
        if word not in df.columns:
            df[word] = None
        df.at[topic, word] = words_count_dict[word]
    

async def async_main():
    global  words_count_dict
    topic_list = read_topic_file(TOPIC_FILE_PATH)
    logging.debug(topic_list)
    tasks = []
 
    shared_data_frame = pd.DataFrame(index=topic_list)
    try:
        # Submit tasks to the pool
        for topic in topic_list:    
            task = asyncio.create_task(word_distribution_task(shared_data_frame, topic))
            logging.debug("future has created")
            tasks.append(task)


        print(f"===========> Task list:{tasks} \n Now waiting for all tasks to complete")

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        shared_data_frame.to_csv('word_distribution.csv', index=True)
        print("=====================> Update word_distribution csv file")
    except Exception as e:
        print(f"Unknown Error: {e}")



@tracer
def main():
    asyncio.run(async_main())


main()
