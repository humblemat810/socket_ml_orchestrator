from celery_redis_word_count import count_words
from celery import Celery
import celery

# Create a Celery instance and load the configuration from celeryconfig.py
celery = Celery('wordcount')
celery.config_from_object('celeryconfig')

from redis import Redis
import logging
class MyNullHandler(logging.Handler):
    def emit(self, record):
        pass
    def handle(self, record):
        pass
    def createLock(self):
        self.lock = None

# Remove the default StreamHandler from the root logger
logging.getLogger().handlers.clear()

# Create a custom NullHandler
null_handler = MyNullHandler()

# Add the NullHandler to the root logger
logging.getLogger().addHandler(null_handler)
logger = logging.getLogger()

# Connect to Redis
redis = Redis(host='localhost', port=6379, db=0)
import threading
words = ["apple", "banana", "cherry", "orange", "grape", "kiwi"]
import time
import logging
stop_event = threading.Event()
from collections import Counter

from celery_redis_word_count import count_words

def dispatch_from_main():
    last_run = time.time()
    cnt = 0
    final_counter = Counter()
    while not stop_event.is_set():
        cnt+=1
        import random
        def generate_random_word():
            return random.choice(words)
        text = generate_random_word()#{"frame_number": cnt, "face": np.random.rand(96,96,6), 'mel': np.random.rand(80,16)}
        result = count_words.delay(text)
        mycnt = result.get(timeout=1)
        myCnt = Counter()
        for k, v in mycnt.items():
            myCnt[k] = v
        final_counter = final_counter + myCnt
        now_time = time.time()
        logger.debug(f'__main__ : cnt {cnt} added, interval = {now_time - last_run}')
        last_run = now_time
    for k,v in final_counter.items():
        print(k,v)
th = threading.Thread(target = dispatch_from_main)
th.start()
time.sleep(10)
stop_event.set()
# Submit the Celery task



# Store the word counts in Redis
#redis.hmset('word_counts', result)