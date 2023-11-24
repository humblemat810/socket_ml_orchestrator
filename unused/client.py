import socket
import hashlib
import pickle
# Create a socket
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server
server_address = ('localhost', 5000)


def echo_frame():
    cnt = 0
    while True:
        # Receive data and checksum
        try:
            client_socket.connect(server_address)
            data_with_checksum = client_socket.recv(231628*10)
            if not data_with_checksum:
                break

            # Split data and checksum
            checksum = data_with_checksum[:32].decode()
            received_data = data_with_checksum[32:]
            #data = pickle.loads(received_data)
            # Calculate checksum of received data
            calculated_checksum = hashlib.md5(received_data).hexdigest()

            # Verify checksum
            if checksum == calculated_checksum:
                #file.write(received_data)  # Write data to file
                print(f'{cnt}checksum correct calculated_checksum={calculated_checksum}')

                cnt += 1
                client_socket.sendall(data_with_checksum)
            else:
                print("Checksum mismatch. Data corrupted.")
        except ConnectionResetError:
            # TO_DO: graceful stop
            
            pass
echo_frame()
# Close the socket
client_socket.close()