import logging


import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description='Example program to accept a port number.')

# Add the port argument
parser.add_argument('--port', type=int, help='Port number', dest = 'port')

# Parse the command-line arguments
args = parser.parse_args()

# Access the port number
port = args.port
import os
os.environ['port'] = str(port)
# Create a logger
logger = logging.getLogger(f"server_ml_echo_worker-{port}")
logger.setLevel(logging.DEBUG)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler(f'server_ml_echo_worker{port}.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug('start loading module server_ml_echo_worker.py')



if __name__ == "__main__":
    from pytaskqml.utils.sample.worker_demo import word_count_worker
    my_ML_socket_server = word_count_worker(server_address = ('localhost', port))
    my_ML_socket_server.start()
