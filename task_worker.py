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
class base_socket_worker(base_worker):
    def __init__(self, server_address = ('localhost', 5000),
                 min_time_lapse_ns = 10000, *arg, **kwarg):
        self.server_address = server_address
        self.min_time_lapse_ns = min_time_lapse_ns
        self.stop_flag = threading.Event()
        self.start_time = None
        self.send_queue = Queue()
        self.received_parsed_queue = Queue()
        logging.basicConfig(filename='worker_{server_address[0]}{server_address[1]}.log', level=logging.INFO)
        
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
        
    
    def send(self,data):
        self.send_queue.put(data)
    def handle_send(self, client_socket: socket.socket):
        packet_cnt = 0
        self.last_run = time.time() - self.min_time_lapse_ns/ 1e9
        while not self.stop_flag.is_set():
            try:
                serialized_data = self.send_queue.get()
                if serialized_data is Stop_Signal:
                    break
                self._send(client_socket, serialized_data)
                packet_cnt += 1
                self.last_run = time.time()
            except ConnectionResetError:
                # TO-DO graceful stop
                self.send_queue.put(Stop_Signal)
                client_socket.close()
                return
                
            except OSError:
                self.send_queue.put(Stop_Signal)
                client_socket.close()
                return
                
            except Exception as e:
                self.send_queue.put(Stop_Signal)
                client_socket.close()
                return
            except ConnectionAbortedError:
                client_socket.close()
                return
    def workload(self):
        # implement data deserialising and processing here
        pass
    def handle_workload(self):
        while True:
            parsed_data = self.received_parsed_queue.get()
            if parsed_data is Stop_Signal: 
                break
            self.send(self.workload(parsed_data))
            
    def graceful_stop():

        pass
    def handle_receive(self, client_socket: socket):
        
        data_with_checksum = bytes()
        socket_last_run = time.time()
        self.socket_start_time = time.time()
        cnt = 0
        while not self.stop_flag.is_set():
            try:
                # readable, _, _ = select.select([client_socket], [], [])
                # for readable_socket in readable:
                #     # sleep to wait for data coming in to save the thread

                #     pass
                data_with_checksum = data_with_checksum + client_socket.recv(115757000)
                #print(f"receive non cond: {len(data_with_checksum)}")
                while len(data_with_checksum) >= 115757:
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
                        data_with_checksum = data_with_checksum[42+length:]
                        self.received_parsed_queue.put(received_data)
                        time_now =  time.time() 
                        uptime = time_now- self.socket_start_time
                        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                        #file.write(received_data)  # Write data to file
                        cnt += 1
                        logging.info(f'uptime: {uptime_str}, packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                        socket_last_run = time_now
                    else:
                        logging.info("receive cond Checksum mismatch. Data corrupted.")
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
        pass
    def handle_socket(self,client_socket):
        
        th_recv = threading.Thread(target = self.handle_receive, args = [client_socket], name = "handle_receive")
        th_recv.start()
        # loop process reopen socket
        th_process = threading.Thread(target = self.handle_workload, args = [], name = "handle_workload")
        th_process.start()
        # loop send
        th_send = threading.Thread(target = self.handle_send, args = [client_socket], name = "handle_send")
        th_send.start()

        th_recv.join()
        th_process.join()
        th_send.join()
        pass
    def start(self, server_address = None, is_server = True):
        if server_address is None:
            server_address = self.server_address
        else:
            self.server_address = server_address
        self.start_time = time.time()
        
        while not self.stop_flag.is_set():
            if is_server:
                try:
                    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Bind the socket to a specific address and port
                    if self.server_address:
                        self.server_address = self.server_address
                    self.server_socket.bind(self.server_address)
                    print("socket server started")
                    
                except:
                    self.server_socket.close()
                    continue
                while not self.stop_flag.is_set():
                    try:
                        self.server_socket.listen(1)

                        # Accept a client connection
                        client_socket, client_address = self.server_socket.accept()
                        print(f"accepted client {client_address}")
                        th = threading.Thread(target = self.handle_socket, args = [client_socket])
                        th.start()
                    except Exception as e:
                        print(e)
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
