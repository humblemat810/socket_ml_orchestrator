import socket
import time

import  threading

class ReconnectingSocket:
    def __init__(self, host_port):
        host, port = host_port
        self.host = host
        self.port = port
        self.sock = None
        self.reconnecting_needed = threading.Event()
        self.reconnecting_lock = threading.Lock()
        self.retry_enabled = True
        self.th_reconnecting_watchdog = threading.Thread(
                    target = self.reconnecting_watchdog,
                    name = f'reconnecting_watchdog-{self.host}:{self.port}')
        self.last_reconnect_time = time.time()
        self.reconnect_interval = 2
        
    def reconnecting_watchdog(self):
        while self.retry_enabled:
            try:
                self.reconnecting_needed.wait(timeout = 2)
            except RuntimeError:
                continue
            except Exception as e:
                print(e)
                pass
            try:    
                self.reconnecting_lock.acquire(timeout = 2)
            except RuntimeError:
                continue
            except Exception as e:
                print(e)
                pass
            if ((time.time() - self.last_reconnect_time > self.reconnect_interval) and 
                self.reconnecting_needed.is_set()):
                self.connect()
                self.last_reconnect_time = time.time()
                self.reconnecting_needed.clear()
            try:
                self.reconnecting_lock.release()
            except RuntimeError as e:
                e
    def connect(self):
        if not self.th_reconnecting_watchdog.is_alive():
            self.th_reconnecting_watchdog = threading.Thread(
                    target = self.reconnecting_watchdog,
                    name = f'reconnecting_watchdog-{self.host}:{self.port}')
            self.th_reconnecting_watchdog.start()

        while self.retry_enabled:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(5.0)
                self.sock.connect((self.host, self.port))
                print("Socket connected successfully")
                break  # Exit the loop if connection is successful
            except socket.error as e:
                import traceback
                traceback.print_exc()
                print(e)
                print("Socket connection failed. Retrying in 1 second...")
                
                time.sleep(self.reconnect_interval)

    def sendall(self, data):
        while self.retry_enabled:
            try:
                self.sock.sendall(data)
                #print("Data sent successfully\n").then
                break  # Exit the loop if send is successful
            except socket.error as se:
                print("Socket send failed. Reconnecting...")
                #self.connect()
                if type(se) is socket.timeout:
                    # ok, no data in yet
                    continue
                with self.reconnecting_lock:
                    self.reconnecting_needed.set()
                time.sleep(self.reconnect_interval)
    def recv(self, buffer_size):
        while self.retry_enabled:
            try:
                data = self.sock.recv(buffer_size)
                #print("Data received successfully\n")
                return data  # Exit the loop and return received data
            except socket.error as se:
                if type(se) is socket.timeout:
                    # ok, no data in yet
                    continue
                print("Socket receive failed. Reconnecting...")
                with self.reconnecting_lock:
                    self.reconnecting_needed.set()
                time.sleep(self.reconnect_interval)
                #self.connect()
    def close(self):
        self.sock.close()