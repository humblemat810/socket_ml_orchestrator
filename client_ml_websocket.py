import asyncio
import websockets



import socket
import hashlib

import numpy as np
import time
import lipsync_schema_pb2
import threading
import uuid

import time
# Create a socket

min_time_lapse_ns = 0
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
server_address = ('localhost', 5000)

stop_flag = threading.Event()
# Listen for incoming connections


async def handle_send(client_socket):
    last_run = time.time()
    while not stop_flag.is_set():
        try:
            message = lipsync_schema_pb2.RequestData()
            message.messageuuid=str(uuid.uuid1())
            message.face.extend(np.random.rand(96,96,3).flatten()) 
            message.mel.extend(np.random.rand(80,16).flatten())
            serialized_data = message.SerializeToString()
            # Calculate checksum
            length = len(serialized_data) 
            if length > 1e10:
                raise ValueError("data frame size cannot be greater thatn 1e10 bytes")
            length = str(length).zfill(10).encode()
            checksum = hashlib.md5(length + serialized_data).hexdigest()
            #time.sleep(0.0005)
            # Send data and checksum
            await client_socket.send(checksum.encode() + length + serialized_data)
            
            cur_time = time.time()
            time_diff = min_time_lapse_ns / 1e9 - cur_time + last_run
            last_run = cur_time
            if time_diff > 0:
                time.sleep(time_diff)
            
        except websockets.exceptions.ConnectionClosedError:
            # TO-DO graceful stop
            await client_socket.close()
            break
            
async def handle_receive(client_socket: socket.socket):
    cnt = 0
    data_with_checksum = bytes()
    while not stop_flag.is_set():
        try:
            """checksum = left_over + client_socket.recv(max(32-len(left_over), 0))
            hash_algo = hashlib.md5()
            
            checksum = checksum.decode()
            length = client_socket.recv(10)
            hash_algo.update(length)
            length = int(length.decode())
            received_data = client_socket.recv(length)
            hash_algo.update(received_data)
            calculated_checksum = hash_algo.hexdigest()"""
            data_with_checksum = data_with_checksum + await client_socket.recv()
            while len(data_with_checksum) >= 115757:
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
                    uptime = time.time() - start_time
                    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
                    #file.write(received_data)  # Write data to file
                    cnt += 1
                    print(f'uptime: {uptime_str}, packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                    
                else:
                    print("Checksum mismatch. Data corrupted.")
                    await client_socket.close()
                    break
        except websockets.exceptions.ConnectionClosedError:
            # TO-DO graceful stop
            await client_socket.close()
            return

async def handle_socket():
    # Send the serialized data over the network or any other transport mechanism
    async with websockets.connect('ws://localhost:12346') as websocket:
        
        task = asyncio.create_task(handle_send(websocket))
        task2 = asyncio.create_task(handle_receive(websocket))

        results = asyncio.gather(task, task2)
        results2 = await results
        print(results)
start_time = time.time()


asyncio.get_event_loop().run_until_complete(handle_socket())
asyncio.get_event_loop().run_forever()