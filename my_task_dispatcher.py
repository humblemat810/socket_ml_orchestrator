from task_dispatcher import Worker, Worker_Sorter, Task_Worker_Manager
import socket
import time
from threading import Thread

class My_Worker(Worker):
    def __init__(self, is_local, remote_info):
        super().__init__()
        self.is_local = is_local
        if is_local:
            self.client_socket = None
        else:
            self.server_address = remote_info # eg example : ('localhost', 5000)
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            def client_connect(server_address):
                try:
                    client_socket.connect(server_address)
                except:
                    time.sleep(4)
            th = Thread(target = client_connect, args=[self.server_address])
            th.start()
    def reduce(self, *arg):
        task_id, (frame_number, result) = arg
        # skip self.queue_dispatched
        with self.worker_manager.q_task_completed.not_full:
            self.worker_manager.q_task_completed.put((task_id, (frame_number, result)))
                    
        
    def map(self):
        super().__init__()
        #run on thread
        if self.local:
            with self.queue_pending.not_empty:
                frame_number, (task_id, data) = self.queue_pending._first()
                result = call_ml(data=data)
                def call_ml(data):
                    pass
                self.reduce(frame_number, task_id, result)
                
            #task_id, data = self.get_next_task()
            
            # add data to local processing thread
            # return self remote call
            
        else:
            # add data to remote socket manager thread
            # send data frame to worker via socket
            pass
        