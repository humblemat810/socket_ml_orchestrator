from pytaskqml.task_worker import base_socket_worker

class echo_worker(base_socket_worker):
    def workload(self, received_data):
        from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import plain_string_message_pb2
        request_array = plain_string_message_pb2.MyMessage()
        request_array.ParseFromString(received_data)
        request_array.my_field = "hello" + request_array.my_field
        response_data_array = request_array
        serialized_data = response_data_array.SerializeToString()
        return serialized_data
    
class word_count_worker(base_socket_worker):

    def workload(self, received_data):
        from pytaskqml.utils.sample.serialisers.plain_string_message_pb2 import MyMessage
        request_message = MyMessage()
        request_message.ParseFromString(received_data)
        
        import pickle
        from collections import Counter
        serialized_data = pickle.dumps({'uuid': request_message.messageuuid, 'counts': Counter(request_message.strmessage)})
        return serialized_data
    
class torch_worker():
    pass