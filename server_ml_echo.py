
import socket
import hashlib
import time
import numpy as np
import threading, inspect
import logging


class ML_socket_server():
    def __init__(self, min_time_lapse_ns = 0, 
                 server_address = ('localhost', 5000)):
        self.min_time_lapse_ns = min_time_lapse_ns
# Create a socket
        self.server_socket = None
        self.server_address = server_address
        logging.basicConfig(filename='worker_{server_address[0]}{server_address[1]}.log', level=logging.INFO)
        self.stop_flag = threading.Event()
    def workload(self):
        function_name = inspect.currentframe().f_code.co_name
        raise(NotImplementedError(f"this class method {function_name} need to be overriden"))
    def start(self, server_address = None):
        if server_address is not None:
            self.server_address = server_address
        while True:
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to a specific address and port
                if self.server_address:
                    self.server_address = self.server_address
                self.server_socket.bind(self.server_address)
                logging.info("socket server started")
                while not self.stop_flag.is_set():

                    self.server_socket.listen(1)

                    # Accept a client connection
                    client_socket, client_address = self.server_socket.accept()
                    logging.info(f"accepted client {client_address}")
                    th = threading.Thread(target = self.echo_ml_request, args = [client_socket])
                    th.start()
            except:
                self.server_socket.close()
                continue
    def echo_ml_request(self, client_socket):
        # Send the serialized data over the network or any other transport mechanism
        global model
        data_with_checksum = bytes()
        cnt = 0
        while True:
            # Receive data and checksum
            try:
                
                data_with_checksum = data_with_checksum + client_socket.recv(2263490000)
                while len(data_with_checksum) >= 226349:
                    last_run = time.time() - self.min_time_lapse_ns/ 1e9
                    hash_algo = hashlib.md5()
                    
                    checksum = data_with_checksum[:32].decode()
                    length = data_with_checksum[32:42]
                    hash_algo.update(length)
                    length = int(data_with_checksum[32:42].decode())
                    received_data = data_with_checksum[42:42+length]
                    hash_algo.update(received_data)
                    calculated_checksum = hash_algo.hexdigest()
                    current_raw_binary = data_with_checksum[:42+length]
                    data_with_checksum = data_with_checksum[42+length:]
                    
                    #data = pickle.loads(received_data)
                    # Calculate checksum of received data
                    

                    # Verify checksum
                    if checksum == calculated_checksum:
                        #file.write(received_data)  # Write data to file
                        logging.info(f'{cnt}checksum correct={calculated_checksum}, data_with_checksum len={len(data_with_checksum)}')

                        cnt += 1
                        
                        serialized_data = self.workload(received_data)
                        length = len(serialized_data)
                        hash_algo = hashlib.md5()
                        length = str(length).zfill(10).encode()
                        hash_algo.update(length)
                        hash_algo.update(serialized_data)
                        calculated_checksum = hash_algo.hexdigest()
                        cur_time = time.time()
                        time_diff = self.min_time_lapse_ns / 1e9 - cur_time + last_run
                        
                        if time_diff > 0:
                            time.sleep(time_diff)
                        client_socket.sendall(calculated_checksum.encode() + length + serialized_data)
                        last_run = cur_time
                    else:
                        logging.info("Checksum mismatch. Data corrupted.")
                
            except ConnectionResetError:
                # TO_DO: graceful stop
                logging.info('ConnectionResetError, closing socket, leaving thread')
                client_socket.close()
                break
            except ConnectionAbortedError:
                logging.info('ConnectionAbortedError, closing socket,  leaving thread')
                client_socket.close()
                break

