from pytaskqml.task_worker import base_socket_worker
class ml_echo_worker(base_socket_worker):
    def workload(self, received_data):
        request_array = lipsync_schema_pb2.RequestDataArray()
        
        response_data_array = lipsync_schema_pb2.ResponseDataArray()


        serialized_data = response_data_array.SerializeToString()
        return serialized_data


class echo_worker():
    pass



class torch_worker():
    pass