import logging

# Create a logger
logger = logging.getLogger()

from pytaskqml.task_dispatcher import Socket_Producer_Side_Worker, Task_Worker_Manager, Local_Thread_Producer_Sider_Worker
from pytaskqml.task_dispatcher import Socket_Input_Task_Worker_Manager
import time
import threading

class my_word_count_dispatcher(Task_Worker_Manager):
    def __init__(self, *arg, **kwarg):
        super().__init__(*arg, **kwarg)
        from collections import Counter
        self.count_dict = Counter()
    def on_shutdown(self):
        for k,v in self.count_dict.items():
            print(k,v)
        return super().on_shutdown()

    def send_output(self, fresh_output_minibatch):
        for task_info, counter in fresh_output_minibatch:
            self.count_dict += counter
            # for k in counter.keys():
            #     print(k,self.count_dict.get(k))
        pass
    pass

class my_word_count_socket_input_dispatcher(Socket_Input_Task_Worker_Manager, 
                                            my_word_count_dispatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checksum_on = False
    def deserialise_parsed_input(self, received_data):
        from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
        request_message = MyMessage()
        request_message.ParseFromString(received_data)
        return {"task_id": request_message.messageuuid, "data" : request_message.strmessage}
        
def my_word_count_config_aware_worker_abstract_factory_getter(*args, **kwargs):
    from functools import partial
    from pytaskqml.task_dispatcher import config_aware_worker_factory
    return partial(config_aware_worker_factory, 
                   local_factory = my_word_count_local_producer_side_worker, 
                   remote_factory = my_word_count_socket_producer_side_worker)
class my_word_count_local_producer_side_worker(Local_Thread_Producer_Sider_Worker):
    def workload(self, task_info, task_data):
        import re
        from collections import Counter
        delimiters = [",", ";", "|", "."]

# Create a regular expression pattern by joining delimiters with the "|" (OR) operator
        pattern = "|".join(map(re.escape, delimiters))

        # Split the string using the pattern as the delimiter
        cnt = Counter(re.split(pattern, task_data))
        return cnt
    
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
        
# if __name__ == '__main__':
#     worker_config = [#{"location": "local"},
#                      {"location": ('localhost', 12345)}, 
#                     ]
#     my_task_worker_manager = Task_Worker_Manager(
#                 worker_factory=my_word_count_socket_producer_side_worker, 
#                 worker_config = worker_config,
#                 output_minibatch_size = 24,
#                 management_port = 8000
#             )
    
#     th = threading.Thread(target = my_task_worker_manager.start, args = [], name = 'dispatcher side demo classes, main')
#     th.start()

#     # this demo shows how to write own loop to dispatch data

#     logger.debug('signal start')
#     def dispatch_from_main():
#         # this function demonstrate main dispatch data such as video frame data or ref to video frame for example
#         cnt = 0
#         while not my_task_worker_manager.stop_flag.is_set():
#             cnt+=1
#             frame_data = {'data_ref','k'+str(cnt)}#{"frame_number": cnt, "face": np.random.rand(96,96,6), 'mel': np.random.rand(80,16)}
#             my_task_worker_manager.dispatch(frame_data)
#             time.sleep(0.02)
#             logger.debug(f'__main__ : cnt {cnt} added')

#     th = threading.Thread(target = dispatch_from_main, args = [], name='dispatch_from_main')
#     th.start()
#     while not my_task_worker_manager.stop_flag.is_set():
#         time.sleep(0.4)
#     while True:
#         try:
#             my_task_worker_manager.graceful_stopped.wait(timeout=2)
#         except:
#             continue
#         break
