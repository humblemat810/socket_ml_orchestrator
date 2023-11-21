import logging
import traceback
# Create a logger
logger = logging.getLogger("server_ml_echo_worker")
logger.setLevel(logging.DEBUG)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler('server_ml_echo_worker.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug('start loading module task_worker.py')


import socket
import hashlib
import numpy as np
import time
import lipsync_schema_pb2
import threading
import uuid
from queue import Queue
import select
import time
import logging
from reconnecting_socket import ReconnectingSocket
debug = True
class base_worker():
    def __init__():
        pass
    def handle_send():
        pass
    def handle_receive():
        pass
    pass
class Stop_Signal():
    pass
class myclient():
    def __init__(self, client_socket, id, watchdog_timeout = 5):
        self.client_socket  = client_socket
        self.stop_event : threading.Event = threading.Event()
        self.received_parsed_queue = Queue()
        self.send_queue = Queue()
        self.id = id
        self.watchdog_timeout = watchdog_timeout
        self.creation_time = time.time()
        self.last_received = time.time()
        self.th_timeout_watchdog = threading.Thread(target = self.timeout_watcherdog, name = f"timeout watchdog {self.id}")
        #self.th_timeout_watchdog.start()
    def timeout_watcherdog(self):
        while True:
            time.sleep(self.watchdog_timeout)
            if time.time() - self.last_received > self.watchdog_timeout:
                logger.debug(f'killed connection client id = {self.id}')
                self.client_socket.close()
                self.stop_event.set()
                self.send_queue.put(Stop_Signal)
                
class base_socket_worker(base_worker):
    def __init__(self, server_address = ('localhost', 5000),
                 min_time_lapse_ns = 10000, *arg, **kwarg):
        self.server_address = server_address
        self.min_time_lapse_ns = min_time_lapse_ns
        self.stop_flag = threading.Event()
        self.start_time = None
        #self.send_queue = Queue()
        self.client_dict = {}
        #self.received_parsed_queue = Queue()
        
        
    def _send(self, client_socket, serialized_data):
        
        
        length = len(serialized_data)
        hash_algo = hashlib.md5()
        length = str(length).zfill(10).encode()
        hash_algo.update(length)
        hash_algo.update(serialized_data)
        calculated_checksum = hash_algo.hexdigest()
        cur_time = time.time()
        time_diff = self.min_time_lapse_ns / 1e9 - cur_time + self.last_run
        
        if time_diff > 0:
            time.sleep(time_diff)
        client_socket.sendall(calculated_checksum.encode() + length + serialized_data)
        self.last_run = cur_time
        
    
    def send(self,client: myclient, data):
        client.send_queue.put(data)
    def handle_send(self, client: myclient):
        logger.debug(f"client {client.id} handle_send start")
        packet_cnt = 0
        self.last_run = time.time() - self.min_time_lapse_ns/ 1e9
        while not client.stop_event.is_set():
            try:
                serialized_data = client.send_queue.get()
                if serialized_data is Stop_Signal:
                    logger.info("handle_send stop signal")
                    client.stop_event.set()
                    client.client_socket.close()
                    break
                logger.debug(f"client {client.id} _send start")
                self._send(client.client_socket, serialized_data)
                logger.debug(f"client {client.id} _send finish, total sent {packet_cnt}")
                packet_cnt += 1
                self.last_run = time.time()
            except ConnectionResetError as e:
                # TO-DO graceful stop
                
                logger.debug(traceback.format_exc())
                client.send_queue.put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                return
                
            except OSError as e:
                logger.debug(traceback.format_exc())
                client.send_queue.put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                return
                
            except Exception as e:
                logger.debug(traceback.format_exc())
                client.send_queue.put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                return
            except ConnectionAbortedError as e:
                logger.debug(traceback.format_exc())
                client.send_queue.put(Stop_Signal)
                client.stop_event.set()
                client.close()
                return
        print(f"client {client.id} exited handle_send while loop")
    def workload(self):
        # implement data deserialising and processing here
        pass
    def handle_workload(self, client:myclient):
        while not client.stop_event.is_set():
            logger.debug(f"client {client.id} handle_workload waiting get_parsed_data")
            parsed_data = client.received_parsed_queue.get()
            if parsed_data is Stop_Signal: 
                break
            logger.debug(f"client {client.id} handle_workload get_parsed_data pre-workload")
            data = self.workload(parsed_data)
            logger.debug(f"client {client.id} handle_workload workload sending back results")
            self.send(client = client, data=data)
            logger.debug(f"client {client.id} handle_workload sent back results")
        print(f"client {client.id} exited handle_workload while loop")
            
    def graceful_stop():

        pass
    def handle_receive(self, client: myclient):
        # wait on socket receive lock
        logger.debug(f"client {client.id} handle_receive start")
        data_with_checksum = bytes()
        socket_last_run = time.time()
        self.socket_start_time = time.time()
        cnt = 0
        while not client.stop_event.is_set():
            try:
                # readable, _, _ = select.select([client_socket], [], [])
                # for readable_socket in readable:
                #     # sleep to wait for data coming in to save the thread

                #     pass
                logger.debug(f"client {client.id} handle_receive socket receive waiting")
                data_with_checksum = data_with_checksum + client.client_socket.recv(115757000)
                logger.debug(f"client {client.id} handle_receive socket received len{len(data_with_checksum)}")
                client.last_received = time.time()
                #print(f"receive non cond: {len(data_with_checksum)}")
                while len(data_with_checksum) >= 115757:
                    
                    #print(f"receive cond: {len(data_with_checksum)}")
                    logger.debug(f"client {client.id} handle_receive checking data integrity")
                    hash_algo = hashlib.md5()
                    
                    checksum = data_with_checksum[:32].decode()
                    length = data_with_checksum[32:42]
                    hash_algo.update(length)
                    length = int(data_with_checksum[32:42].decode())
                    received_data = data_with_checksum[42:42+length]
                    hash_algo.update(received_data)
                    calculated_checksum = hash_algo.hexdigest()
                    
                    if checksum == calculated_checksum:
                        logger.debug(f"client {client.id} handle_receive data correct")
                        data_with_checksum = data_with_checksum[42+length:]
                        client.received_parsed_queue.put(received_data)
                        time_now =  time.time() 
                        uptime = time_now- self.socket_start_time
                        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                        #file.write(received_data)  # Write data to file
                        cnt += 1
                        logging.info(f'uptime: {uptime_str}, correct packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                        socket_last_run = time_now
                    else:
                        logging.info("receive cond Checksum mismatch. Data corrupted.")
                        client.close()
                        return
            except ConnectionResetError as e:
                # TO-DO graceful stop
                logger.debug(traceback.format_exc())
                client.client_socket.close()
                client.stop_event.set()
                return
            except UnicodeDecodeError as e:
                logger.debug(traceback.format_exc())
                client.client_socket.close()
                client.stop_event.set()
                return
            except OSError as e:
                logger.debug(traceback.format_exc())
                client.client_socket.close()
                client.stop_event.set()
                return
            except ConnectionAbortedError as e:
                logger.debug(traceback.format_exc())
                client.send_queue.put(Stop_Signal)
                client.stop_event.set()
                client.close()
                return
            except Exception as e:
                logger.debug(traceback.format_exc())
                import traceback
                traceback.print_exc(e)
                client.stop_event.set()
                client.client_socket.close()
                return
        print(f"client {client.id} exited handle_receive while loop")
        pass
    
    def handle_socket(self,client):
        logger.debug(f"client {client.id} handle_socket socket receive waiting")
        th_recv = threading.Thread(target = self.handle_receive, args = [client], name = "handle_receive")
        th_recv.start()
        # loop process reopen socket
        th_process = threading.Thread(target = self.handle_workload, args = [client], name = "handle_workload")
        th_process.start()
        # loop send
        th_send = threading.Thread(target = self.handle_send, args = [client], name = "handle_send")
        th_send.start()

        th_recv.join()
        th_process.join()
        th_send.join()
        logger.debug(f"client {client.id} handle_socket client {client.id} exits")
    def start(self, server_address = None, is_server = True):
        if server_address is None:
            server_address = self.server_address
        else:
            self.server_address = server_address
        self.start_time = time.time()
        if debug:
            # spawn a debug thread for debugger insertion
            # to debug case when all 3 rec, send, process are sleeping
            from threading import Thread
            def debug_fun():
                while True:
                    time.sleep(1)
            th_debug = Thread(target =debug_fun)
            th_debug.start()
        while not self.stop_flag.is_set():
            if is_server:
                try:
                    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Bind the socket to a specific address and port
                    if self.server_address:
                        self.server_address = self.server_address
                    self.server_socket.bind(self.server_address)
                    logger.info("start socket server started")
                    
                except:
                    self.server_socket.close()
                    continue
                cnt_client_id = 0
                while not self.stop_flag.is_set():
                    try:
                        self.server_socket.listen(1)

                        # Accept a client connection
                        client_socket, client_address = self.server_socket.accept()
                        
                        logger.info(f"start accepted client {client_address}")
                        client = myclient(client_socket, id=cnt_client_id)
                        self.client_dict[cnt_client_id] = client
                        cnt_client_id += 1
                        th = threading.Thread(target = self.handle_socket, args = [client])
                        th.start()
                    except Exception as e:
                        logger.debug(traceback.format_exc())
                        pass
            else:
                client_socket = ReconnectingSocket(server_address)
                try:
                    client_socket.connect(server_address)
                except ConnectionRefusedError:
                    time.sleep(2)
                    continue
                
                self.handle_socket(client_socket)
            
            
                client_socket.close()
