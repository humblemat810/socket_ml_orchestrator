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
        time.sleep(11.5)
        import requests
        import threading
        def url_request_with_my_err_handling(url):
            cnt_retry = 0
            cnt_retry_max = 3
            errs= []
            while True:
                try:
                    requests.get(url)
                except ConnectionRefusedError: # server port shutdown
                    break
                except requests.exceptions.ConnectionError as e:
                    import urllib3
                    if isinstance(e.args[0],  urllib3.exceptions.ProtocolError):
                        uePE = e.args[0]
                        if uePE.args[0] == 'Connection aborted.':
                            if isinstance(uePE.args[1], ConnectionResetError):
                                CRE = uePE.args[1]
                                if CRE.args[0] == 10054 and CRE.args[1] == 'An existing connection was forcibly closed by the remote host':
                                    break # ok for this error
                    elif isinstance(e.args[0],  urllib3.exceptions.MaxRetryError):
                        ueMRE = e.args[0]
                        if isinstance(ueMRE.reason, urllib3.exceptions.NewConnectionError):
                            break
                    else:
                        raise
                except Exception as e: # network disrupt, server busy
                    cnt_retry += 1
                    errs.append(e)
                    if cnt_retry > cnt_retry_max:
                        print(errs)
                        raise
                    continue
            return True
        def shutdown_server():
            return url_request_with_my_err_handling('http://localhost:18000/shutdown')
        def shutdown_worker(i_worker):
            return url_request_with_my_err_handling(f'http://localhost:{22345+i_worker}/shutdown')
            
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
    main(n_worker= 3)
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
