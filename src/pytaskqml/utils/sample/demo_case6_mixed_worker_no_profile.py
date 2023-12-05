import logging
class MyNullHandler(logging.Handler):
    def emit(self, record):
        pass
    def handle(self, record):
        pass
    def createLock(self):
        self.lock = None

# Remove the default StreamHandler from the root logger
logging.getLogger().handlers.clear()

# Create a custom NullHandler
null_handler = MyNullHandler()

# Add the NullHandler to the root logger
logging.getLogger().addHandler(null_handler)

import multiprocessing
import subprocess

import pathlib
def run_worker(i_worker = 0):
    # Define the arguments for the first Python file
    args_file1 = ["python",
                  #'-o', str((pathlib.Path(".")/'worker12345.ystat').resolve()),  
                  r"demo_case1_wordcount_worker.py", "--port", str(12345 + i_worker), "--management-port", str(22345+0), "--config", 'worker.ini']
    subprocess.call(args_file1)

def run_dispatcher(n_worker):
    # Define the arguments for the second Python file
    args_file2 = ["python" ,
                  #'-o', str((pathlib.Path(".")/'dispatcher.ystat').resolve()), 
                  r"demo_case4_dispatcher.py", "--management-port", "18000", "--config", 'dispatcher_case5.ini', '--n-worker', f"{n_worker}"]
    subprocess.call(args_file2)

import os

class ChangeDirectory:
    def __init__(self, new_directory):
        self.new_directory = new_directory
        self.previous_directory = None

    def __enter__(self):
        self.previous_directory = os.getcwd()
        os.chdir(self.new_directory)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.previous_directory)
def shutdown(dispatcher_worker_config):
    import threading
    from pytaskqml.utils.management import shutdown_url_request
    def shutdown_server():
        return shutdown_url_request('http://localhost:18000/shutdown')
    def shutdown_worker(i_worker):
        return shutdown_url_request(f'http://localhost:{22345+i_worker}/shutdown')
        
    threading.Thread(target = shutdown_server).start() # worker
    from typing import List
    shutdown_ths: List[threading.Thread] = []
    for i, worker_config in enumerate((dispatcher_worker_config)):
        if worker_config['location'] == 'local':
            pass
        else:
            # shutdown remote worker
            shutdown_ths.append(threading.Thread(target = shutdown_worker, args = [i], name = f'shutdown worker {i}'))
    shutdown_ths.append(threading.Thread(target = shutdown_server, name = 'shutdown server'))
    for th in shutdown_ths:
        th.start()
    for th in shutdown_ths:
        th.join()
def start_processes(dispatcher_worker_config):
    n_worker = len(dispatcher_worker_config)
    import os, pathlib
    with ChangeDirectory(str(pathlib.Path(__file__).parent)):
        processes = []
        remote_worker_cnt = 0
        for i, worker_config in enumerate((dispatcher_worker_config)):
            if worker_config['location'] == 'local':
                pass
            else:
                processes.append(multiprocessing.Process(target = run_worker, args = [remote_worker_cnt]))
                remote_worker_cnt += 1
        processes.append(multiprocessing.Process(target=run_dispatcher, args = [n_worker]))
        # Start both processes
        for p in processes:
            p.start()
def main(dispatcher_worker_config):
    start_processes(dispatcher_worker_config)
    import time
    time.sleep(11.5)
    shutdown(dispatcher_worker_config)
    print('done')
import argparse
parser = argparse.ArgumentParser(description="Demo case 4")
parser.add_argument('--n-worker', default = None)
args = parser.parse_args()
if __name__ == "__main__":
    import yappi
    yappi.set_clock_type("WALL")
    yappi.start()
    import configparser
    dispatcher_parser = configparser.ConfigParser()
    dispatcher_parser.read("dispatcher_case5.ini")
    from pytaskqml.utils.confparsers import dispatcher_side_worker_config_parser
    worker_configs = dispatcher_side_worker_config_parser(dispatcher_parser, args)
    main(dispatcher_worker_config = worker_configs)
    yappi.stop()
    import pytaskqml
    from pytaskqml import task_dispatcher, task_worker
    stats0 = yappi.get_thread_stats()
    print('func_stats')
    stats = yappi.get_func_stats(
        filter_callback=lambda x: yappi.module_matches(x, [pytaskqml, task_dispatcher, task_worker])
    )
    stats0.print_all()
    stats.print_all()
    print('temp')