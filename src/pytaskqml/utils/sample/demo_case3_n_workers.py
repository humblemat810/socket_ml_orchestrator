import multiprocessing
import subprocess

import pathlib
def run_worker(i_worker = 0):
    # Define the arguments for the first Python file
    args_file1 = ["python",'yappi_main_wrapper.py', 
                  #'-o', str((pathlib.Path(".")/'worker12345.ystat').resolve()),  
                  r"demo_case1_wordcount_worker.main", f"worker{12345+i_worker}", "--port", str(12345 + i_worker), "--management-port", str(22345+0), "--config", 'worker.ini']
    subprocess.call(args_file1)

def run_dispatcher(n_worker):
    # Define the arguments for the second Python file
    args_file2 = ["python",'yappi_main_wrapper.py', 
                  #'-o', str((pathlib.Path(".")/'dispatcher.ystat').resolve()), 
                  r"demo_case1_dispatcher.main", 'dispatcher',"--management-port", "18000", "--config", 'dispatcher_case2.ini', '--n-worker', f"{n_worker}"]
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
def main(n_worker = 1):
    import os, pathlib
    with ChangeDirectory(str(pathlib.Path(__file__).parent)):
        

        processes = []

        for i in range(n_worker):
            processes.append(multiprocessing.Process(target = run_worker, args = [i]))
        processes.append(multiprocessing.Process(target=run_dispatcher, args = [n_worker]))
        
        # Start both processes
        for p in processes:
            p.start()
        
        import time
        time.sleep(3)
        import requests
        import threading
        def shutdown_server():
            while True:
                try:
                    requests.get('http://localhost:18000/shutdown')
                except:
                    continue
                return
        def shutdown_worker(i_worker):
            while True:
                try:
                    requests.get(f'http://localhost:{22345+i_worker}/shutdown')
                except:
                    continue
                return
        
        threading.Thread(target = shutdown_server).start() # worker
        from typing import List
        shutdown_ths: List[threading.Thread] = []
        for i in range(n_worker):
            shutdown_ths.append(threading.Thread(target = shutdown_worker, args = [i], name = f'shutdown worker {i}'))
        shutdown_ths.append(threading.Thread(target = shutdown_server, name = 'shutdown server'))
        for th in shutdown_ths:
            th.start()
        for th in shutdown_ths:
            th.join()
        # Wait for both processes to finish

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
