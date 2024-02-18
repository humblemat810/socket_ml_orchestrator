import logging

# Create a logger
logger = logging.getLogger()

from pytaskqml.task_dispatcher import Socket_Producer_Side_Worker, Task_Worker_Manager, Local_Thread_Producer_Sider_Worker
from pytaskqml.task_dispatcher import Socket_Input_Task_Worker_Manager
import time
import threading

class my_echo_dispatcher(Task_Worker_Manager):
    def __init__(self,*arg, **kwarg):
        super().__init__(*arg, **kwarg)
        self.result_cnt = 0
        
    def send_output(self, fresh_output_minibatch):
        for task_info, data in fresh_output_minibatch:
            self.result_cnt+=1
            print(data)
    def on_shutdown(self):
        print(self.result_cnt)
        return super().on_shutdown()
import pickle
from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
class my_echo_socket_producer_side_worker(Socket_Producer_Side_Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_proto = False
    def parse_task_info(self, single_buffer_result):
        if self.use_proto:
        
            response_array = MyMessage()
            response_array.ParseFromString(single_buffer_result)
            messageuuid = response_array.messageuuid
            strmessage = response_array.strmessage
        else:
            deserialised_data = pickle.loads(single_buffer_result)
            messageuuid, strmessage = deserialised_data['messageuuid'], deserialised_data['strmessage']
        return (messageuuid, strmessage)
    
    def dispatch(self, task_info, *args, **kwargs):
        # this has to be implemented to tell what exactly to do to dispatch data to worker
        # such as what protocol e.g. protobuf, to use to communicate with worker and also #
        # transfer data or ref to data
        if self.use_proto:
            message = MyMessage()
            message.messageuuid = task_info[0][1]
            message.strmessage = task_info[1]
            serialized_data = message.SerializeToString()
        else:
            serialized_data = pickle.dumps({"messageuuid": task_info[0][1], "strmessage": task_info[1]})
        return serialized_data
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
    def parse_task_info(self,single_buffer_result):
        uuid = single_buffer_result[0][0]
        data = single_buffer_result[1]
        return single_buffer_result
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
    def parse_task_info(self, single_buffer_result):
        import pickle
        pkload = pickle.loads(single_buffer_result)
        uuid = pkload['uuid']
        counts = pkload['counts']
        return (uuid, counts)


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
        