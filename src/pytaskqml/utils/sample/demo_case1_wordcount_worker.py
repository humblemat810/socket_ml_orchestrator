import logging
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description='Demo worker')

# Add the port argument
parser.add_argument('--port', type=int, help='Port number', dest = 'port', default = "12345")
parser.add_argument('--management-port', type=str, help='Management Port number', dest = 'management_port', default = "8001")
parser.add_argument('--log-level', dest="log_level")
parser.add_argument('--config', help='Configuration file path')

parser.add_argument('--log-screen', action='store_const', const=True, default=False, help='Enable log to screen', dest="log_screen")

# Parse the command-line arguments
args = parser.parse_args()
import configparser
config = configparser.ConfigParser()
if args.config:    
    config.read(args.config)

management_port = args.management_port
if management_port is None:
    management_port = config.get("worker", "management-port")
management_port = int(management_port)

port = args.port 
if port is None:
    port = config.get("worker", "port")    
port = int(port)
log_level = args.log_level
if log_level is None:
    log_level = config.get("logger", "level")
log_screen = args.log_screen
if log_screen is None:
    log_screen = config.get("logger", "logscreen")
    
if type(log_screen) is str:
    if log_screen.upper() == 'FALSE':
        log_screen = False
    elif log_screen.upper() == 'TRUE':
        log_screen = True
    else:
        raise ValueError(f"incorrect config for log_screen, expected UNION[FALSE, TRUE], get {log_screen}")
import os
os.environ['port'] = str(port)
# Create a logger
logger = logging.getLogger(f"server_ml_echo_worker-{port}")
logger.setLevel(log_level)

# Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the console handler to the logger
if log_screen:
    logger.addHandler(console_handler)

# fh = logging.FileHandler(f'server_ml_echo_worker{port}.log', mode='w', encoding='utf-8')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# logger.addHandler(fh)
from pytaskqml.utils.logutils import QueueFileHandler
#log_file_name = config.get("logger", "file")
qfh = QueueFileHandler(f"server_ml_echo_worker{port}.log")
qfh.setLevel(log_level)
qfh.setFormatter(formatter)
logger.addHandler(qfh)
logger.debug('start loading module')


from worker_demo_classes import word_count_worker
def main():
    import sys
    print(sys.modules[__name__])
    my_ML_socket_server = word_count_worker(server_address = ('localhost', int(port) ),
                                            management_port=management_port, min_start_processing_length = 42)
    my_ML_socket_server.start()
    my_ML_socket_server.graceful_stop_done.wait()
    qfh.stop_flag.set()
    print(f'worker{port} stopped')
if __name__ == "__main__":
    main()
    

