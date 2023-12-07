import queue

import logging
# Custom handler for async queue processing
class QueueHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = queue.Queue()
    def emit(self, record):
        self.queue.put(record)
import threading
import time
from collections import deque
class QueueFileHandler(QueueHandler):
    def __init__(self,file_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_name = file_name
        self.queue = queue.Queue()
        self.th_write = threading.Thread(target=self.write_loop, args = [self.file_name, self.queue], name = 'logger-asyncwriter')
        self.stop_flag = threading.Event()
        self.th_write.start()
        
    def write_loop(self, file_name, q: queue.Queue):
        while not self.stop_flag.is_set():
            time.sleep(2)
            new_deque = deque()
            q.mutex.acquire()
            new_deque, q.queue = q.queue, new_deque
            q.mutex.release()
            with open(file_name, 'a') as f:
                while len(new_deque) > 0:
                    f.write(self.formatter.format(new_deque.popleft()) + "\n")
        

    def emit(self, record):
        super().emit(record)