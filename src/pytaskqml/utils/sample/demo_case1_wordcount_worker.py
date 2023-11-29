import logging
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description='Demo worker')

# Add the port argument
parser.add_argument('--port', type=int, help='Port number', dest = 'port', default = "12345")
parser.add_argument('--management-port', type=int, help='Management Port number', dest = 'management_port', default = "8001")


# Parse the command-line arguments
args = parser.parse_args()
management_port = args.management_port
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
logger.debug('start loading module')


from worker_demo_classes import word_count_worker

if __name__ == "__main__":
    
    my_ML_socket_server = word_count_worker(server_address = ('localhost', port),
                                            management_port=management_port, min_start_processing_length = 42)
    my_ML_socket_server.start()

