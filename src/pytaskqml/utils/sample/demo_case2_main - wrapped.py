import multiprocessing
import subprocess

import pathlib
def run_worker1():
    # Define the arguments for the first Python file
    args_file1 = ["python",'yappi_main_wrapper.py', 
                  #'-o', str((pathlib.Path(".")/'worker12345.ystat').resolve()),  
                  r"demo_case1_wordcount_worker.main", 'worker12345', "--port", "12345", "--management-port", "22345", "--config", 'worker.ini']
    subprocess.call(args_file1)
def run_worker2():
    # Define the arguments for the first Python file
    args_file1 = ["python",'yappi_main_wrapper.py', 
                  #'-o', str((pathlib.Path(".")/'worker12346.ystat').resolve()),  
                  r"demo_case1_wordcount_worker.main", 'worker12346', "--port", "12346", "--management-port", "22346", "--config", 'worker2.ini']
    subprocess.call(args_file1)
def run_dispatcher():
    # Define the arguments for the second Python file
    args_file2 = ["python",'yappi_main_wrapper.py', 
                  #'-o', str((pathlib.Path(".")/'dispatcher.ystat').resolve()), 
                  r"demo_case1_dispatcher.main", 'dispatcher',"--management-port", "18000", "--config", 'dispatcher_case2.ini']
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
        time.sleep(4)
        import requests
        import threading
        def shutdown_server():
            while True:
                try:
                    requests.get('http://localhost:18000/shutdown')
                except:
                    continue
                return
        def shutdown_worker1():
            while True:
                try:
                    requests.get('http://localhost:22345/shutdown')
                except:
                    continue
                return
        def shutdown_worker2():
            while True:
                try:
                    requests.get('http://localhost:22346/shutdown')
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
    import yappi
    yappi.set_clock_type("WALL")
    yappi.start()
    main()
    yappi.stop()
    import pytaskqml
    from pytaskqml import task_dispatcher, task_worker
    stats0 = yappi.get_thread_stats()
    
    stats = yappi.get_func_stats(
        filter_callback=lambda x: yappi.module_matches(x, [pytaskqml, task_dispatcher, task_worker])
    )
    stats0.print_all()
    stats.print_all()
    print('temp')
