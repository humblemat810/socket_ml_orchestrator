from celery import Celery

# Create a Celery instance

celery = Celery('celery_redis_word_count', backend='redis://localhost:6379', broker='redis://localhost:6379/0')
import re
import hashlib
@celery.task
def count_words(task_data):
    hash_algo = hashlib.md5()
                    
    checksum = task_data[:32].decode()
    length = task_data[32:42]

    hash_algo.update(length)
    length = int(task_data[32:42].decode())
    received_data = task_data[42:42+length]
    hash_algo.update(received_data)
    calculated_checksum = hash_algo.hexdigest()
    if calculated_checksum != checksum:
        raise ConnectionError('Data Corruption: Checksum Mismatch')
    from collections import Counter
    delimiters = [",", ";", "|", "."]

# Create a regular expression pattern by joining delimiters with the "|" (OR) operator
    pattern = "|".join(map(re.escape, delimiters))

        # Split the string using the pattern as the delimiter
    cnt = Counter(re.split(pattern, received_data))
    
    return cnt

#start script:  celery -A tasks worker --loglevel=info

