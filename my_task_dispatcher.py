import logging

# Create a logger
logger = logging.getLogger("my_task_dispatcher")
logger.setLevel(logging.DEBUG)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s",

                              )
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler('my_task_dispatcher.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)



from task_dispatcher import Worker, Worker_Sorter, Task_Worker_Manager
from task_dispatcher import print_all_queues # debug tools
from pprint import pprint
import socket, select
from reconnecting_socket import ReconnectingSocket
import time
from threading import Thread, Event
import hashlib
import lipsync_schema_pb2, uuid
import numpy as np
import queue
debug = False
def is_socket_connected(sock):
    # Check if the socket is connected
    try:
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        return True
    except socket.error:
        return False
class My_Worker(Worker):
    def init_socket(self):
        pass
    def __init__(self, is_local, remote_info, *arg, **kwarg):
        
        super().__init__(*arg, **kwarg)
        self.uuid_to_time = {}
        self.min_time_lapse_ns = 10000
        self.packet_cnt = 0
        self.last_run = time.time() - self.min_time_lapse_ns/ 1e9
        self.is_local = is_local
        self.auto_call_reduce = False
        self.stop_flag = Event()
        self.socket_last_run = None
        if is_local:
            self.client_socket = None
        else:
            self.server_address = remote_info # eg example : ('localhost', 5000)
            self.client_socket = ReconnectingSocket(self.server_address)
            self.client_socket.connect()
            th = Thread(target = self.result_buffer_collect, args = [], name = f'result buffer collect {self.id}')
            self.socket_start_time = time.time()
            th.start()
        self.map_result_buffer = queue.Queue()
        self.watermark = time.time()
        self.output_minibatch_size = 24
    def graceful_stop(self):
        super().graceful_stop()
        if self.is_local:
            pass
        else:
            self.client_socket.retry_enabled = False
        
    def result_buffer_collect(self):
        
        
        client_socket = self.client_socket
        client_socket.sock.settimeout(2)
        data_with_checksum = bytes()
        self.socket_last_run = time.time()
        cnt = 0
        while not self.stop_flag.is_set():
            try:
                # readable, _, _ = select.select([self.client_socket.sock], [], [])
                # for readable_socket isn readable:
                #     # sleep to wait for data coming in to save the thread

                #     pass
                try:
                    data = client_socket.recv(115757000)
                except socket.timeout as te:
                    continue # check for stop event
                
                data_with_checksum = data_with_checksum + data
                logger.debug('worker data received')
                #print(f"receive non cond: {len(data_with_checksum)}")
                while not self.stop_flag.is_set() and (len(data_with_checksum) >= 110000):
                    #print(f"receive cond: {len(data_with_checksum)}")
                    hash_algo = hashlib.md5()
                    
                    checksum = data_with_checksum[:32].decode()
                    length = data_with_checksum[32:42]
                    hash_algo.update(length)
                    length = int(data_with_checksum[32:42].decode())
                    received_data = data_with_checksum[42:42+length]
                    hash_algo.update(received_data)
                    calculated_checksum = hash_algo.hexdigest()
                    
                    if checksum == calculated_checksum:
                        try:
                            self.map_result_buffer.put(received_data, timeout = 1)
                        except queue.Full:
                            continue
                        data_with_checksum = data_with_checksum[42+length:]
                    
                        time_now =  time.time() 
                        # uptime = time_now- self.socket_start_time
                        # uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                        #file.write(received_data)  # Write data to file
                        cnt += 1
                        logger.debug(f'worker_data integrity confirmed packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                        self.socket_last_run = time_now
                    else:
                        logger.debug("from worker receive cond Checksum mismatch. Data corrupted.")
                        client_socket.close()
                        return
            except ConnectionResetError as e:
                # TO-DO graceful stop
                client_socket.close()
                return
            except UnicodeDecodeError:
                client_socket.close()
                return
            except OSError:
                client_socket.close()
                return
            except Exception as e:
                import traceback
                traceback.print_exc(e)
                client_socket.close()
                return
            
    def reduce(self, *arg, **kwarg):
        #merge data to face 
        pass
    def _map_result_watcher(self):
        while not self.stop_flag.is_set():
            logger.debug('start listen map result')
            from queue import Empty
            try:
                result = self.map_result_buffer.get(timeout=1)
            except Empty:
                continue
            logger.debug('map result read')
            response = lipsync_schema_pb2.ResponseData()
            response.ParseFromString(result) 
            result = {'messageuuid': response.messageuuid, 'face': np.array(response.face).reshape(96,96,3)}
            if response.messageuuid not in self.uuid_to_time:
                # previous run result stuck on socket buffer
                print(f"previous run result stuck on socket buffer response.messageuuid={response.messageuuid}")
                continue
            task_info = (self.uuid_to_time[response.messageuuid], response.messageuuid)
            self._reduce(map_result = result, task_info = task_info)
            
        
    def prepare_for_dispatch(self,task_info,  *args, **kwargs):
        return task_info
        
    def dispatch(self, *args, **kwargs):
        logger.debug(f"Worker{self.id}.dispatch started dispatch")
        message = lipsync_schema_pb2.RequestData()
        #message.messageuuid=str(uuid.uuid1())
        message.messageuuid=str(args[0][0][1])
        self.uuid_to_time[str(args[0][0][1])] = args[0][0][0]
        
        message.face.extend(np.random.rand(96,96,3).flatten()) 
        message.mel.extend(np.random.rand(80,16).flatten())
        serialized_data = message.SerializeToString()
        logger.debug(f"Worker{self.id}.dispatch serialised")
        # Calculate checksum
        length = len(serialized_data) 
        if length > 1e10:
            raise ValueError("send: data frame size cannot be greater thatn 1e10 bytes")
        length = str(length).zfill(10).encode()
        checksum = hashlib.md5(length + serialized_data).hexdigest()
        #time.sleep(0.0005)
        # Send data and checksum
        cur_time = time.time()
        
        time_diff = self.min_time_lapse_ns / 1e9 - cur_time + self.last_run
        #print('send: cur_time ', cur_time, ", last_run ", last_run, ", length ", length)
        if time_diff > 0:
            logger.debug(f"Worker{self.id}.dispatch sleep for {time_diff}")
            time.sleep(time_diff)
        self.packet_cnt+=1
        if self.client_socket is not None:
            logger.debug(f"Worker{self.id}.dispatch sending serialied data")
            self.client_socket.sendall(checksum.encode() + length + serialized_data)
            logger.debug(f"Worker{self.id}.dispatch sent serialied data")
        self.last_run = cur_time
        
        pass
import select      
if __name__ == '__main__':
    worker_config = [#{"location": "local"},
                     {"location": ('localhost', 12345)}, 
                     #{"location": ('localhost', 12346)}, 
                     #{"location": ("192.168.1.2", 12345)}
                    ]
    my_task_worker_manager = Task_Worker_Manager(worker_factory=My_Worker, 
                                                 worker_config = worker_config,
                                                 output_minibatch_size = 24
                                                 )
    
    th = Thread(target = my_task_worker_manager.start, args = [])
    th.start()

    

    logger.debug('signal start')
    def dispatch_from_main():
        cnt = 0
        while not my_task_worker_manager.stop_flag.is_set():
            cnt+=1
            frame_data = {'data_ref','k'+str(cnt)}#{"frame_number": cnt, "face": np.random.rand(96,96,6), 'mel': np.random.rand(80,16)}
            my_task_worker_manager.dispatch(frame_data)
            
            #print_all_queues(my_task_worker_manager)
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
