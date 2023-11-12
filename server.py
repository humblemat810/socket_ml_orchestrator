import socket
import hashlib
import numpy as np
import pickle
import time
import numpy as np
import lipsync_schema_pb2
import uuid
import threading

# Create a DataMessage instance
message = lipsync_schema_pb2.RequestData()
message.messageuuid=str(uuid.uuid1())
message.face.extend(np.random.rand(96,96,3).flatten()) 
message.mel.extend(np.random.rand(80,16).flatten())
serialized_data = message.SerializeToString()

data = pickle.dumps((np.random.rand(96,96,3),np.random.rand(80,16)))
# Create a socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to a specific address and port
server_address = ('localhost', 5000)
server_socket.bind(server_address)

stop_flag = threading.Event()
# Listen for incoming connections
def handle_client_send(client_socket):
    while not stop_flag.is_set():
        try:
            message = lipsync_schema_pb2.RequestData()

            message.face.extend(np.random.rand(96,96,3).flatten()) 
            message.mel.extend(np.random.rand(80,16).flatten())
            serialized_data = message.SerializeToString()
            # Calculate checksum
            checksum = hashlib.md5(serialized_data).hexdigest()
            time.sleep(0.001)
            # Send data and checksum
            client_socket.sendall(checksum.encode() + serialized_data)
        except ConnectionResetError:
            # TO-DO graceful stop
            raise
            pass
def handle_client_receive(client_socket: socket.socket):
    cnt = 0
    while not stop_flag.is_set():
        try:
            data_with_checksum = client_socket.recv(231628*10)
            checksum = data_with_checksum[:32].decode()
            received_data = data_with_checksum[32:]
            calculated_checksum = hashlib.md5(received_data).hexdigest()
            if checksum == calculated_checksum:
                #file.write(received_data)  # Write data to file
                cnt += 1
                print(f'{cnt}checksum correct calculated_checksum={calculated_checksum}')
                
            else:
                print("Checksum mismatch. Data corrupted.")
        except ConnectionResetError:
            # TO-DO graceful stop
            raise
            pass

def handle_client(client_socket):
    # Send the serialized data over the network or any other transport mechanism
    th = threading.Thread(target = handle_client_send, args = [client_socket])
    th2 = threading.Thread(target = handle_client_receive, args = [client_socket])
    th.start()
    th2.start()

while not stop_flag.is_set():

    server_socket.listen(1)

    # Accept a client connection
    client_socket, client_address = server_socket.accept()
    th = threading.Thread(target = handle_client, args = [client_socket])
    th.start()




# Close the sockets
client_socket.close()
server_socket.close()