import socket
import time

from threading import Event, Condition, Lock, Thread

class ReconnectingSocket:
    def __init__(self, host_port):
        host, port = host_port
        self.host = host
        self.port = port
        self.sock = None
        self.reconnecting_needed = Event()
        self.reconnecting_lock = Lock()
        self.th_reconnecting_watchdog = Thread(target = self.reconnecting_watchdog)
        #self.th_reconnecting_watchdog.start()
        self.last_reconnect_time = time.time()
        self.reconnect_interval = 2
    def reconnecting_watchdog(self):
        while True:
            self.reconnecting_needed.wait()
            with self.reconnecting_lock:
                if time.time() - self.last_reconnect_time < self.reconnect_interval:
                    self.connect()
                    self.last_reconnect_time = time.time()
                    self.reconnecting_needed.clear()
    def connect(self):
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                print("Socket connected successfully")
                break  # Exit the loop if connection is successful
            except socket.error as e:
                print(e)
                print("Socket connection failed. Retrying in 1 second...")
                
                time.sleep(self.reconnect_interval)

    def sendall(self, data):
        while True:
            try:
                self.sock.sendall(data)
                #print("Data sent successfully\n").then
                break  # Exit the loop if send is successful
            except socket.error:
                print("Socket send failed. Reconnecting...")
                #self.connect()
                with self.reconnecting_lock:
                    self.reconnecting_needed.set()
                time.sleep(self.reconnect_interval)
    def recv(self, buffer_size):
        while True:
            try:
                data = self.sock.recv(buffer_size)
                #print("Data received successfully\n")
                return data  # Exit the loop and return received data
            except socket.error:
                print("Socket receive failed. Reconnecting...")
                with self.reconnecting_lock:
                    self.reconnecting_needed.set()
                time.sleep(self.reconnect_interval)
                #self.connect()
    def close(self):
        self.sock.close()