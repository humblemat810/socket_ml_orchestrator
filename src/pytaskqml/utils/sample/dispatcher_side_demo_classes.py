import logging

# Create a logger
logger = logging.getLogger()

from pytaskqml.task_dispatcher import Socket_Producer_Side_Worker, Task_Worker_Manager
import time
from threading import Thread

class my_word_count_socket_producer_side_worker(Socket_Producer_Side_Worker):
    
    def _parse_task_info(self, single_buffer_result):
        import pickle
        pkload = pickle.loads(single_buffer_result)
        uuid = pkload['uuid']
        counts = pkload['counts']
        task_time = self.uuid_to_time.get(uuid)
        if task_time is not None:
            task_info = (self.uuid_to_time[uuid], uuid)
            success = True
            map_result = counts
        else: # previous retry already solved in a race condition
            success = False
            task_info = None
            map_result = None
        
        return success, task_info, map_result
    def dispatch(self, task_info, *args, **kwargs):
        # this has to be implemented to tell what exactly to do to dispatch data to worker
        # such as what protocol e.g. protobuf, to use to communicate with worker and also #
        # transfer data or ref to data
        from pytaskqml.utils.sample.serialisers import plain_string_message_pb2
        message = plain_string_message_pb2.MyMessage()
        message.messageuuid = task_info[0][1]
        message.strmessage = task_info[1]
        serialized_data = message.SerializeToString()
        
        return serialized_data
        
if __name__ == '__main__':
    worker_config = [#{"location": "local"},
                     {"location": ('localhost', 12345)}, 
                    ]
    my_task_worker_manager = Task_Worker_Manager(
                worker_factory=my_word_count_socket_producer_side_worker, 
                worker_config = worker_config,
                output_minibatch_size = 24,
                management_port = 8000
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
            frame_data = {'data_ref','k'+str(cnt)}#{"frame_number": cnt, "face": np.random.rand(96,96,6), 'mel': np.random.rand(80,16)}
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
