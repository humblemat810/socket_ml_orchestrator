from pytaskqml.task_worker import base_socket_worker
import pickle
from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
class echo_worker(base_socket_worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_proto = True
    def workload(self, received_data):
        import time
        start_time = time.time()
        if self.use_proto:
        
        
            request_array = MyMessage()
            request_array.ParseFromString(received_data)
            
            
            response_data = MyMessage()
            response_data.strmessage = "hello" + request_array.strmessage
            response_data.messageuuid = request_array.messageuuid
            serialized_data = response_data.SerializeToString()
        else:
        ##################===========
            deserialised_data = pickle.loads(received_data)
            deserialised_data['strmessage'] =  "hello" + deserialised_data['strmessage']
            serialized_data = pickle.dumps(deserialised_data)
            end_time = time.time()
            self.logger.debug(f"workload takes time {end_time - start_time}")
        return serialized_data
    
class char_count_worker(base_socket_worker):

    def workload(self, received_data):
        from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
        request_message = MyMessage()
        request_message.ParseFromString(received_data)
        
        import pickle
        from collections import Counter
        serialized_data = pickle.dumps({'uuid': request_message.messageuuid, 'counts': Counter(request_message.strmessage)})
        return serialized_data

class word_count_worker(base_socket_worker):

    def workload(self, received_data):
        from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
        request_message = MyMessage()
        request_message.ParseFromString(received_data)
        
        import pickle
        import re
        from collections import Counter
        delimiters = [",", ";", "|", "."]

# Create a regular expression pattern by joining delimiters with the "|" (OR) operator
        pattern = "|".join(map(re.escape, delimiters))

        # Split the string using the pattern as the delimiter
        serialized_data = pickle.dumps({'uuid': request_message.messageuuid, 'counts': Counter(re.split(pattern, request_message.strmessage))})
        return serialized_data


class torch_worker():
    pass