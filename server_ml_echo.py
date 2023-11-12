import socket
import hashlib
import numpy as np
import time
import numpy as np
import lipsync_schema_pb2
import uuid
import threading
request = lipsync_schema_pb2.RequestData()
response = lipsync_schema_pb2.ResponseData()
class ML_socket_server():
    def __init__(self, min_time_lapse_ns = 0, 
                 server_address = ('localhost', 5000)):
        self.min_time_lapse_ns = min_time_lapse_ns
# Create a socket
        self.server_socket = None
        self.server_address = server_address
        self.stop_flag = threading.Event()
    def start(self, server_address = None):
        while True:
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to a specific address and port
                if self.server_address:
                    self.server_address = self.server_address
                self.server_socket.bind(self.server_address)
                while not self.stop_flag.is_set():

                    self.server_socket.listen(1)

                    # Accept a client connection
                    client_socket, client_address = self.server_socket.accept()
                    th = threading.Thread(target = self.echo_ml_request, args = [client_socket])
                    th.start()
            except:
                self.server_socket.close()
                raise
    def echo_ml_request(self, client_socket):
        # Send the serialized data over the network or any other transport mechanism
        data_with_checksum = bytes()
        cnt = 0
        while True:
            # Receive data and checksum
            try:
                
                data_with_checksum = data_with_checksum + client_socket.recv(1157990000)
                while len(data_with_checksum) >= 115799:
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
                        print(f'{cnt}checksum correct calculated_checksum={calculated_checksum}, data_with_checksum len={len(data_with_checksum)}')

                        cnt += 1
                        
                        request.ParseFromString(received_data) #231546
                        face = np.array(request.face).reshape([96,96,3])
                        mel = np.array(request.mel).reshape([80,16])
                        face = face[list(reversed(range(96))), : ,: ]
                        response = lipsync_schema_pb2.ResponseData()
                        response.messageuuid = request.messageuuid
                        response.face.extend(face.flatten())
                        serialized_data = response.SerializeToString()
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
                        print("Checksum mismatch. Data corrupted.")
                
            except ConnectionResetError:
                # TO_DO: graceful stop
                print('Connection reset error')
                client_socket.close()
                break
            except ConnectionAbortedError:
                client_socket.close()
                break

    

if __name__ == "__main__":
    my_ML_socket_server = ML_socket_server()
    my_ML_socket_server.start()


