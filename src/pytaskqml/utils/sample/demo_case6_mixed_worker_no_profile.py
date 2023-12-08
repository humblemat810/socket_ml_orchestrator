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
    p = subprocess.call(args_file1)
    print(f'worker {i_worker} subprocess done with return code {p}')

def run_dispatcher(n_worker):
    # Define the arguments for the second Python file
    args_file2 = ["python" ,
                  #'-o', str((pathlib.Path(".")/'dispatcher.ystat').resolve()), 
                  r"demo_case4_dispatcher.py", "--management-port", "18000", "--config", 'dispatcher_case5.ini', '--n-worker', f"{n_worker}"]
    p = subprocess.call(args_file2)
    print(f'dispatcher with {n_worker} workers done with return code {p}')

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
        response = shutdown_url_request('http://localhost:18000/shutdown')
        print("dispatcher", 'response', response)
        return response
    def shutdown_worker(i_worker, i_remote_worker):
        print(f"shutting down {i_remote_worker}th remote worker", i_worker)
        response = shutdown_url_request(f'http://localhost:{22345+i_remote_worker}/shutdown')
        print(f"{i_worker} worker, {i_remote_worker} remote worker", 'response', response)
        return response
    threading.Thread(target = shutdown_server).start() # worker
    from typing import List
    shutdown_ths: List[threading.Thread] = []
    remote_worker_cnt = 0
    for i_worker, worker_config in enumerate((dispatcher_worker_config)):
        if worker_config['location'] == 'local':
            pass
        else:
            # shutdown remote worker
            shutdown_ths.append(threading.Thread(target = shutdown_worker, 
                                                 args = [i_worker, remote_worker_cnt], 
                                                 name = f'shutdown worker {i_worker}'))
            remote_worker_cnt += 1
    shutdown_ths.append(threading.Thread(target = shutdown_server, name = 'shutdown server'))
    for th in shutdown_ths:
        th.start()
    for th in shutdown_ths:
        th.join()
        th.join()
def start_processes(dispatcher_worker_config):
    n_worker = len(dispatcher_worker_config)
    import pathlib
    from typing import List
    with ChangeDirectory(str(pathlib.Path(__file__).parent)):
        processes:List[multiprocessing.Process]  = []
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
    import sys
    sys.exit()
import argparse
parser = argparse.ArgumentParser(description="Demo case 4")
parser.add_argument('--n-worker', default = None)
args = parser.parse_args()
if __name__ == "__main__":
    # import yappi
    # yappi.set_clock_type("WALL")
    # yappi.start()
    import configparser
    dispatcher_parser = configparser.ConfigParser()
    dispatcher_parser.read("dispatcher_case5.ini")
    from pytaskqml.utils.confparsers import dispatcher_side_worker_config_parser
    worker_configs = dispatcher_side_worker_config_parser(dispatcher_parser, args)
    main(dispatcher_worker_config = worker_configs)
    # yappi.stop()
    # import pytaskqml
    # from pytaskqml import task_dispatcher, task_worker
    # stats0 = yappi.get_thread_stats()
    # print('func_stats')
    # stats = yappi.get_func_stats(
    #     filter_callback=lambda x: yappi.module_matches(x, [pytaskqml, task_dispatcher, task_worker])
    # )
    # stats0.print_all()
    # stats.print_all()
    # print('temp')
