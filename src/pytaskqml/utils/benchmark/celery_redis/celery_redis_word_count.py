from celery import Celery

# Create a Celery instance

celery = Celery('celery_redis_word_count', backend='redis://localhost:6379', broker='redis://localhost:6379/0')
import re

@celery.task
def count_words(task_data):
    from collections import Counter
    delimiters = [",", ";", "|", "."]

# Create a regular expression pattern by joining delimiters with the "|" (OR) operator
    pattern = "|".join(map(re.escape, delimiters))

        # Split the string using the pattern as the delimiter
    cnt = Counter(re.split(pattern, task_data))
    
    return cnt

#start script:  celery -A tasks worker --loglevel=info

