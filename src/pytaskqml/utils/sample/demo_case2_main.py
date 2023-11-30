import multiprocessing
import subprocess

import pathlib
def run_worker1():
    # Define the arguments for the first Python file
    args_file1 = ["python",'-m','cProfile', '-o', str((pathlib.Path(".")/'worker12345.prof').resolve()),  r"demo_case1_wordcount_worker.py", "--port", "12345", "--management-port", "22345", "--config", 'worker.ini']
    subprocess.call(args_file1)
def run_worker2():
    # Define the arguments for the first Python file
    args_file1 = ["python",'-m','cProfile', '-o', str((pathlib.Path(".")/'worker12346.prof').resolve()),  r"demo_case1_wordcount_worker.py", "--port", "12346", "--management-port", "22346", "--config", 'worker2.ini']
    subprocess.call(args_file1)
def run_dispatcher():
    # Define the arguments for the second Python file
    args_file2 = ["python",'-m','cProfile', '-o', str((pathlib.Path(".")/'dispatcher.prof').resolve()), r"demo_case1_dispatcher.py", "--management-port", "18000", "--config", 'dispatcher_case2.ini']
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
def main():
    import os, pathlib
    with ChangeDirectory(str(pathlib.Path(__file__).parent)):
        

        # Create two separate processes for running the files
        process1 = multiprocessing.Process(target=run_worker1)
        process2 = multiprocessing.Process(target=run_worker2)
        process3 = multiprocessing.Process(target=run_dispatcher)    
        
        # Start both processes
        process1.start()
        process2.start()
        process3.start()
        import time
        time.sleep(12)
        import requests
        import threading
        def shutdown_server():
            while True:
                try:
                    requests.get('http://localhost:18000/shutdown', timeout = 5)
                except:
                    continue
                return
        def shutdown_worker1():
            while True:
                try:
                    requests.get('http://localhost:22345/shutdown', timeout = 5)
                except:
                    continue
                return
        def shutdown_worker2():
            while True:
                try:
                    requests.get('http://localhost:22346/shutdown', timeout = 5)
                except:
                    continue
                return
        threading.Thread(target = shutdown_server).start() # worker
        threading.Thread(target = shutdown_worker1).start()
        threading.Thread(target = shutdown_worker2).start()
        # Wait for both processes to finish
        process1.join()
        process2.join()
        process3.join()
    print('done')
if __name__ == "__main__":
    main()
    