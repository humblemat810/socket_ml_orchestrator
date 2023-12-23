__all__ = ["base_socket_worker", "base_worker", "Stop_Signal", "myclient", "DataIngressIncomplete", "DataCorruptionError"]
import logging
import traceback
import os
port = os.environ.get('port')
process_id = os.getpid()
class MyNullHandler(logging.Handler):
    def emit(self, record):
        pass
    def handle(self, record):
        pass
    def createLock(self):
        self.lock = None

# Remove the default StreamHandler from the root logger
logging.getLogger().handlers.clear()

# Create a custom NullHandler
null_handler = MyNullHandler()

# Add the NullHandler to the root logger
logging.getLogger().addHandler(null_handler)


import socket
import hashlib

import time
import threading
from queue import Queue, Empty, Full
import time
import logging
from .utils.reconnecting_socket import ReconnectingSocket
from http.server import BaseHTTPRequestHandler, HTTPServer
debug = True
class base_worker():
    def __init__(self):
        pass
    def handle_send(self):
        pass
    def handle_receive(self):
        pass
    pass
class Stop_Signal():
    pass
class myclient():
    def __init__(self, client_socket, id, watchdog_timeout = 5):
        self.logger = logging.getLogger(f"server_ml_echo_worker-{port}")
        self.logger.debug('start loading module task_worker.py')
        self.client_socket: socket.socket  = client_socket
        self.stop_event : threading.Event = threading.Event()
        self.received_parsed_queue = Queue()
        self.send_queue = Queue()
        self.id = id
        self.httpd: HTTPServer = None
        self.handle_send_exiting = threading.Event()
        self.handle_recv_exiting = threading.Event()
        self.handle_wkld_exiting = threading.Event()
        self.th_recv    :threading.Thread = None
        self.th_process :threading.Thread = None
        self.th_send    :threading.Thread = None
        self.watchdog_timeout = watchdog_timeout
        self.creation_time = time.time()
        self.last_received = time.time()
        self.th_timeout_watchdog = threading.Thread(target = self.timeout_watcherdog, name = f"timeout watchdog {self.id}")
        #self.th_timeout_watchdog.start()
    def close(self):
        self.client_socket.close()
    def timeout_watcherdog(self):
        while not self.stop_event.is_set():
            time.sleep(self.watchdog_timeout)
            if time.time() - self.last_received > self.watchdog_timeout:
                self.logger.debug(f'killed connection client id = {self.id}')
                self.client_socket.close()
                self.stop_event.set()
                self.send_queue._put(Stop_Signal)
    def stop_client(self):
        self.stop_event.set()
        self.handle_recv_exiting.wait()
        self.handle_wkld_exiting.wait()
        self.handle_send_exiting.wait()
from typing import Dict
class DataIngressIncomplete(BaseException):
    pass
class DataCorruptionError(BaseException):
    pass
def peek_next_len(data_with_checksum, checksum_on):
    if checksum_on:
        if len(data_with_checksum) < 44:
            raise DataIngressIncomplete
        next_len = data_with_checksum[34:44].decode()
    else:
        if len(data_with_checksum) < 10:
            raise DataIngressIncomplete
        next_len = int(data_with_checksum[:10].decode())
    return next_len
def parse_data(data_with_checksum: bytes, checksum_on: bool):
    if checksum_on:
        hash_algo = hashlib.md5()
        checksum = data_with_checksum[:32].decode()
        length = data_with_checksum[32:42]
        if len(data_with_checksum) < int(length) + 42:
            raise(DataIngressIncomplete('more data to come')) # wait for more data to come
        hash_algo.update(length)
        length = int(data_with_checksum[32:42].decode())
        received_data = data_with_checksum[42:42+length]
        hash_algo.update(received_data)
        calculated_checksum = hash_algo.hexdigest()
        
        if checksum == calculated_checksum:
            #self.logger.debug(f"client {client.id} handle_receive data correct")
            pass
        else:
            raise(DataCorruptionError('data corrupt'))
    else:
        length = int(data_with_checksum[checksum_on*32:checksum_on*32+10].decode())
        if len(data_with_checksum) < int(length) + checksum_on*32+10:
            raise DataIngressIncomplete('more data to come') # wait_for_more_data to come
        received_data = data_with_checksum[checksum_on*32+10:checksum_on*32+10+length]
        calculated_checksum = ""
    return length, received_data, calculated_checksum
class base_socket_worker(base_worker):
    def __init__(self, server_address = ('localhost', 5000),
                 min_time_lapse_ns = 10000,
                 management_port=18464,
                 min_start_processing_length = None,
                 checksum_on=False,
                 *arg, **kwarg):
        self.logger = logging.getLogger(f"server_ml_echo_worker-{port}")
        self.server_address = server_address
        self.min_time_lapse_ns = min_time_lapse_ns
        self.stop_flag = threading.Event()
        self.start_time = None
        #self.send_queue = Queue()
        self.client_dict: Dict[int, myclient] = {}
        self.graceful_stop_done = threading.Event()
        self.management_port = management_port
        self.ingress_lock_on = True  # maybe can set false if only 1 ingress
        self.min_start_processing_length = int(min_start_processing_length)
        self.checksum_on = checksum_on
        #import signal
        #signal.signal(signal.SIGINT, self.graceful_stop)
        #self.received_parsed_queue = Queue()

        
    def _send(self, client_socket: socket.socket, serialized_data: str, checksum_on: bool = False):
        length = len(serialized_data)
        length = str(length).zfill(10).encode()
        cur_time = time.time()
        if checksum_on:
            hash_algo = hashlib.md5()
            hash_algo.update(length)
            hash_algo.update(serialized_data)
            calculated_checksum = hash_algo.hexdigest()
            
            time_diff = self.min_time_lapse_ns / 1e9 - cur_time + self.last_run
            
            if time_diff > 0:
                time.sleep(time_diff)
            checksum = calculated_checksum.encode()
        else:
            checksum = "".encode()
        client_socket.sendall(checksum + length + serialized_data)
        self.logger.info('_send succeed')
        self.last_run = cur_time
    
    
    def send(self, client: myclient = None, data = None):
        client.send_queue.put(data)
    def handle_send(self, client: myclient):
        self.logger.debug(f"client {client.id} handle_send start")
        packet_cnt = 0
        self.last_run = time.time() - self.min_time_lapse_ns/ 1e9
        while not client.stop_event.is_set():
            try:
                try:
                    serialized_data = client.send_queue.get(timeout=1)
                except Empty:
                    continue
                if serialized_data is Stop_Signal:
                    self.logger.info("handle_send stop signal")
                    client.stop_event.set()
                    client.client_socket.close()
                    break
                self.logger.debug(f"client {client.id} _send start")
                self._send(client.client_socket, serialized_data)
                self.logger.debug(f"client {client.id} _send finish, total sent {packet_cnt}")
                packet_cnt += 1
                self.last_run = time.time()
            except ConnectionResetError as e:
                # TO-DO graceful stop
                
                self.logger.debug(traceback.format_exc())
                client.send_queue._put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                break
                
            except OSError as e:
                self.logger.debug(traceback.format_exc())
                client.send_queue._put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                break
                
            except Exception as e:
                self.logger.debug(traceback.format_exc())
                client.send_queue._put(Stop_Signal)
                client.stop_event.set()
                client.client_socket.close()
                break
            except ConnectionAbortedError as e:
                self.logger.debug(traceback.format_exc())
                client.send_queue._put(Stop_Signal)
                client.stop_event.set()
                client.close()
                break
        
        print(f"client {client.id} exited handle_send while loop")
        client.handle_send_exiting.set()
    def workload(self, *arg, **kwarg):
        # implement data deserialising and processing here
        pass
    def handle_workload(self, client:myclient):
        while not client.stop_event.is_set():
            self.logger.debug(f"client {client.id} handle_workload waiting get_parsed_data")
            try:
                parsed_data = client.received_parsed_queue.get(timeout = 1)
            except Empty:
                continue
            if parsed_data is Stop_Signal: 
                break
            self.logger.debug(f"client {client.id} handle_workload get_parsed_data pre-workload")
            data = self.workload(parsed_data)
            self.logger.debug(f"client {client.id} handle_workload workload sending back results")
            self.send(client = client, data=data)
            self.logger.debug(f"client {client.id} handle_workload sent back results")
        print(f"client {client.id} exited handle_workload while loop")
        client.handle_wkld_exiting.set()
    def graceful_stop(self, *args):
        self.logger.info('graceful stopping')
        #stop_ths = [threading.Thread(target=i.stop_client, args = []) for i in self.client_dict.values()]
        stop_ths = list(map(lambda x : threading.Thread(target=x.stop_client, name = f'graceful shutdown {x.id}'), self.client_dict.values()))
        list(map(lambda x : x.start() ,stop_ths))
        list(map(lambda x : x.join() ,stop_ths))      
        self.graceful_stop_done.set()
        return True
    def handle_receive(self, client: myclient, checksum_on = False):
        # wait on socket receive lock
        self.logger.debug(f"client {client.id} handle_receive start")
        data_with_checksum = bytes()
        socket_last_run = time.time()
        self.socket_start_time = time.time()
        cnt = 0
        next_len = None
        while not client.stop_event.is_set():
            try:
                # readable, _, _ = select.select([client_socket], [], [])
                # for readable_socket in readable:
                #     # sleep to wait for data coming in to save the thread

                #     pass
                self.logger.debug(f"client {client.id} handle_receive socket receive waiting")
                try:
                    if next_len:
                        data = client.client_socket.recv(next_len+checksum_on*32+10 - len(data_with_checksum))
                    elif self.min_start_processing_length:
                        data = client.client_socket.recv(self.min_start_processing_length)
                    else:
                        data = client.client_socket.recv(4096)
                except socket.timeout:
                    self.logger.debug(f"client {client.id} handle_receive socket receive timeout")
                    continue
                data_with_checksum = data_with_checksum + data
                self.logger.debug(f"client {client.id} handle_receive socket received len{len(data_with_checksum)}")
                client.last_received = time.time()
                #print(f"receive non cond: {len(data_with_checksum)}")
                while not client.stop_event.is_set() and len(data_with_checksum) >= max(checksum_on*32+10, self.min_start_processing_length):
                    self.logger.info('receive')
                    #print(f"receive cond: {len(data_with_checksum)}")
                    self.logger.debug(f"client {client.id} handle_receive checking data integrity")
                    
                    try:
                        if next_len is None:
                            next_len = peek_next_len(data_with_checksum, checksum_on= self.checksum_on)
                        length, received_data, calculated_checksum = parse_data(data_with_checksum, checksum_on= self.checksum_on)
                        time_now =  time.time() 
                        
                    except DataIngressIncomplete as e:
                        break
                    except DataCorruptionError:
                        self.logger.debug("from dispatcher receive cond Checksum mismatch. Data corrupted. Close connection will handshake again")
                        client.client_socket.close()
                        break
                    data_with_checksum = data_with_checksum[checksum_on*32+10+length:]
                    next_len = None
                    while not client.stop_event.is_set():
                        try:
                            if self.ingress_lock_on:
                                client.received_parsed_queue.put(received_data, timeout = 1)
                            else:
                                client.received_parsed_queue._put(received_data)
                            break
                        except Full:
                            continue
                    if client.stop_event.is_set():
                        continue
                    #file.write(received_data)  # Write data to file
                    cnt += 1
                    uptime = time_now- self.socket_start_time
                    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                    self.logger.info(f'uptime: {uptime_str}, correct packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                    
                    socket_last_run = time_now
                
            except ConnectionResetError as e:
                # TO-DO graceful stop
                self.logger.debug(traceback.print_exc())
                client.client_socket.close()
                client.stop_event.set()
                break
            except UnicodeDecodeError as e:
                self.logger.debug(traceback.print_exc())
                client.client_socket.close()
                client.stop_event.set()
                break
            except OSError as e:
                self.logger.debug(traceback.print_exc())
                client.client_socket.close()
                client.stop_event.set()
                break
            except ConnectionAbortedError as e:
                self.logger.debug(traceback.print_exc())
                client.send_queue._put(Stop_Signal)
                client.stop_event.set()
                client.close()
                break
            except Exception as e:
                self.logger.debug(traceback.print_exc())
                traceback.print_exc()
                client.stop_event.set()
                client.client_socket.close()
                break
        print(f"client {client.id} exited handle_receive while loop")
        client.handle_recv_exiting.set()
    
    def handle_socket(self,client: myclient):
        self.logger.debug(f"client {client.id} handle_socket socket receive waiting")
        client.th_recv = threading.Thread(target = self.handle_receive, args = [client], name = "handle_receive")
        client.th_recv.start()
        # loop process reopen socket
        client.th_process = threading.Thread(target = self.handle_workload, args = [client], name = "handle_workload")
        client.th_process.start()
        # loop send
        client.th_send = threading.Thread(target = self.handle_send, args = [client], name = "handle_send")
        client.th_send.start()

        client.th_recv.join()
        client.th_process.join()
        client.th_send.join()
        self.logger.debug(f"client {client.id} handle_socket client {client.id} exits")
    def start(self, server_address = None, is_server = True):
        if server_address is None:
            server_address = self.server_address
        else:
            self.server_address = server_address
        self.start_time = time.time()
        if debug:
            # spawn a debug thread for debugger insertion
            # to debug case when all 3 rec, send, process are sleeping
            
            def debug_fun():
                while not self.stop_flag.is_set():
                    time.sleep(1)
            th_debug = threading.Thread(target =debug_fun, name = 'debug_fun')
            th_debug.start()
        if is_server:
            mytaskworker = self
            class MyHandler(BaseHTTPRequestHandler):
                def _send_response(self, status_code, message):
                    self.send_response(status_code)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(message.encode('utf-8'))

                def do_GET(self):
                    if self.path == '/shutdown':
                        mytaskworker.stop_flag.set()
                        mytaskworker.logger.info(f"worker pid {process_id} port {port} received graceful shutdown signal")
                        mytaskworker.graceful_stop()
                        
                        self._send_response(200, "Shutdown requested\n")
                        def my_http_shutdown():
                            try:
                                httpd.shutdown()
                            except Exception as e:
                                print(e)
                                raise
                            print('normal shutdown')
                        time.sleep(1)
                        th_stop = threading.Thread(target=my_http_shutdown, name = f'worker{mytaskworker.server_address} httpd shutdown')
                        th_stop.start()
                    else:
                        self._send_response(404, "Not found\n")

            
            server_address = ('', self.management_port)
            httpd = HTTPServer(server_address, MyHandler)
            self.httpd = httpd
            def run_server():
                
                httpd.serve_forever()
            self.th_httpd = threading.Thread(target = run_server, name = f'worker-{server_address}.httpd')
            self.th_httpd.start()
        while not self.stop_flag.is_set():
            if is_server:
                try:
                    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.server_socket.settimeout(5.0)
                    self.server_socket.bind(self.server_address)
                    self.logger.info("start socket server started")
                    
                except:
                    self.server_socket.close()
                    continue
                cnt_client_id = 0
                self.server_socket.settimeout(1)
                while not self.stop_flag.is_set():
                    try:
                        self.server_socket.listen(1)

                        # Accept a client connection
                        client_socket, client_address = self.server_socket.accept()
                        
                        self.logger.info(f"start accepted client {client_address}")
                        client_socket.settimeout(2)
                        client = myclient(client_socket, id=cnt_client_id)
                        self.client_dict[cnt_client_id] = client
                        
                        th = threading.Thread(target = self.handle_socket, 
                                              args = [client], name = f'client{cnt_client_id} handle socket')
                        th.start()
                        cnt_client_id += 1
                    except socket.timeout as te:
                        # its ok
                        pass
                    except Exception as e:
                        self.logger.debug(traceback.format_exc())
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
        if is_server:
            self.th_httpd.join()
            self.logger.info('httpd stopped and joint')
            

    
