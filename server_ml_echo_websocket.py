import asyncio
import websockets




import socket
import hashlib
import numpy as np

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

# Create a socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to a specific address and port
server_address = ('localhost', 5000)
server_socket.bind(server_address)

stop_flag = threading.Event()
request = lipsync_schema_pb2.RequestData()
response = lipsync_schema_pb2.ResponseData()

async def echo_ml_request(client_socket, path):
    # Send the serialized data over the network or any other transport mechanism
    data_with_checksum = bytes()
    cnt = 0
    while True:
        # Receive data and checksum
        try:
            data_with_checksum = data_with_checksum + await client_socket.recv()
            while len(data_with_checksum) >= 115799:
                


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
                left_over = data_with_checksum
                
                
                
                #data = pickle.loads(received_data)
                # Calculate checksum of received data
                

                # Verify checksum
                if checksum == calculated_checksum:
                    #file.write(received_data)  # Write data to file
                    print(f'{cnt}checksum correct calculated_checksum={calculated_checksum}')

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
                    await client_socket.send(calculated_checksum.encode() + length + serialized_data)
                else:
                    print("Checksum mismatch. Data corrupted.")
        except websockets.exceptions.ConnectionClosedError:
            # TO_DO: graceful stop
            print('Connection reset error')
            break
"""
while not stop_flag.is_set():

    server_socket.listen(1)

    # Accept a client connection
    client_socket, client_address = server_socket.accept()
    th = threading.Thread(target = echo_ml_request, args = [client_socket])
    th.start()
"""
start_server2 = websockets.serve(echo_ml_request, 'localhost', 12346)
asyncio.get_event_loop().run_until_complete(start_server2)

asyncio.get_event_loop().run_forever()



"""# Close the sockets
client_socket.close()
server_socket.close()"""