import socket
import hashlib
import numpy as np
import time
import lipsync_schema_pb2
import threading
import uuid

import time

#client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
class ML_socket_client():
    def __init__(self, server_address = ('localhost', 5000),
                 min_time_lapse_ns = 10000):
        self.server_address = server_address
        self.min_time_lapse_ns = min_time_lapse_ns
        self.stop_flag = threading.Event()
        self.start_time = None
        # Listen for incoming connections


    def handle_send(self, client_socket: socket.socket):
        packet_cnt = 0
        last_run = time.time() - self.min_time_lapse_ns/ 1e9
        while not self.stop_flag.is_set():
            try:
                message = lipsync_schema_pb2.RequestData()
                message.messageuuid=str(uuid.uuid1())
                message.face.extend(np.random.rand(96,96,6).flatten()) 
                message.mel.extend(np.random.rand(80,16).flatten())
                serialized_data = message.SerializeToString()
                # Calculate checksum
                length = len(serialized_data) 
                if length > 1e10:
                    raise ValueError("send: data frame size cannot be greater thatn 1e10 bytes")
                length = str(length).zfill(10).encode()
                checksum = hashlib.md5(length + serialized_data).hexdigest()
                #time.sleep(0.0005)
                # Send data and checksum
                cur_time = time.time()
                
                time_diff = self.min_time_lapse_ns / 1e9 - cur_time + last_run
                #print('send: cur_time ', cur_time, ", last_run ", last_run, ", length ", length)
                if time_diff > 0:
                    time.sleep(time_diff)
                packet_cnt+=1
                
                client_socket.sendall(checksum.encode() + length + serialized_data)
                #print("send:  packet ", packet_cnt, ' sent, time_diff ', time_diff)
                last_run = cur_time
                
                
            except ConnectionResetError:
                # TO-DO graceful stop
                client_socket.close()
                return
                
            except OSError:
                client_socket.close()
                return
                
            except Exception as e:
                client_socket.close()
                return
            except ConnectionAbortedError:
                client_socket.close()
                return
    def handle_receive(self, client_socket: socket.socket):
        cnt = 0
        data_with_checksum = bytes()
        while not self.stop_flag.is_set():
            try:

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
                    uptime = time.time() - self.start_time
                    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                    if checksum == calculated_checksum:
                        data_with_checksum = data_with_checksum[42+length:]
                        uptime = time.time() - self.start_time
                        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                        #file.write(received_data)  # Write data to file
                        cnt += 1
                        print(f'uptime: {uptime_str}, packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                        
                    else:
                        #print("receive cond Checksum mismatch. Data corrupted.")
                        client_socket.close()
                        return
            except ConnectionResetError:
                # TO-DO graceful stop
                client_socket.close()
                return
            except UnicodeDecodeError:
                client_socket.close()
                return
            except OSError:
                client_socket.close()
                return
            except:
                client_socket.close()
                return
    def handle_socket(self, client_socket):
        # Send the serialized data over the network or any other transport mechanism
        th = threading.Thread(target = self.handle_send, args = [client_socket])
        th2 = threading.Thread(target = self.handle_receive, args = [client_socket])
        th.start()
        th2.start()
        th.join()
        th2.join()
    def start(self, server_address = None):
        if server_address is None:
            server_address = self.server_address
        self.start_time = time.time()
        while not self.stop_flag.is_set():
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client_socket.connect(server_address)
            except ConnectionRefusedError:
                time.sleep(2)
                continue

            th = threading.Thread(target = self.handle_socket, args = [client_socket])
            th.start()
            time.sleep(1)
            th.join()
            
            
        client_socket.close()

    #client_socket.connect(server_address)
    """try:
        client_socket.connect(server_address)
    except Exception as e:
        print(e) # [WinError 10056] A connect request was made on an already connected socket
        pass"""
    # Accept a client connection

    # Close the socket
if __name__ == "__main__":
    my_ML_socket_client =ML_socket_client()
    my_ML_socket_client.start()