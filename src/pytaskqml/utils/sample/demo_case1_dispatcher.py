import logging

# Create a logger
logger = logging.getLogger("demo_case1")
logger.setLevel(logging.DEBUG)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s",
                              )

console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler('demo_case1.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

import argparse
parser = argparse.ArgumentParser(description="Demo server side for")
parser.add_argument('--management-port', type=int, help='Management Port number', dest = 'management_port', default="8000")
args = parser.parse_args()
management_port = args.management_port
import pytaskqml.task_dispatcher as task_dispatcher
task_dispatcher.modulelogger = logger
from pytaskqml.task_dispatcher import Socket_Producer_Side_Worker, Task_Worker_Manager
from dispatcher_side_demo_classes import my_word_count_socket_producer_side_worker
from threading import Thread
import time
if __name__ == '__main__':
    worker_config = [#{"location": "local"},
                     {"location": ('localhost', 12345), 
                      "min_start_processing_length" : 42}, 
                    ]
    my_task_worker_manager = Task_Worker_Manager(
                worker_factory=my_word_count_socket_producer_side_worker, 
                worker_config = worker_config,
                output_minibatch_size = 24,
                management_port = management_port,
                logger = logger,
                retry_watcher_on = False
            )
    
    th = Thread(target = my_task_worker_manager.start, args = [])
    th.start()

    # this demo shows how to write own loop to dispatch data

    logger.debug('signal start')
    def dispatch_from_main():
        # this function demonstrate main dispatch data such as video frame data or ref to video frame for example
        cnt = 0
        while not my_task_worker_manager.stop_flag.is_set():
            cnt+=1
            import random
            words = ["apple", "banana", "cherry", "orange", "grape", "kiwi"]

            def generate_random_word():
                return random.choice(words)
            frame_data = generate_random_word()#{"frame_number": cnt, "face": np.random.rand(96,96,6), 'mel': np.random.rand(80,16)}
            my_task_worker_manager.dispatch(frame_data)
            time.sleep(0.02)
            logger.debug(f'__main__ : cnt {cnt} added')

    th = Thread(target = dispatch_from_main, args = [], name='dispatch_from_main')
    th.start()
    while not my_task_worker_manager.stop_flag.is_set():
        time.sleep(0.4)
    while True:
        try:
            my_task_worker_manager.graceful_stopped.wait(timeout=2)
        except:
            continue
        break