import multiprocessing
import subprocess

def run_worker():
    # Define the arguments for the first Python file
    args_file1 = ["python", r"demo_case0_echo_worker.py", "--port", "12345", "--management-port", "22345", "--config", 'worker0.ini']
    subprocess.call(args_file1)

def run_dispatcher():
    # Define the arguments for the second Python file
    args_file2 = ["python", r"demo_case0_dispatcher.py", "--management-port", "18000", "--config", 'dispatcher_case0.ini']
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
        process1 = multiprocessing.Process(target=run_worker)
        process2 = multiprocessing.Process(target=run_dispatcher)

        # Start both processes
        process1.start()
        process2.start()
        import time
        
        time.sleep(10)

        import requests
        import threading
        def shutdown_server():
            while True:
                try:
                    requests.get('http://localhost:18000/shutdown', timeout = 5)
                except:
                    continue
                return
        def shutdown_worker():
            while True:
                try:
                    requests.get('http://localhost:22345/shutdown', timeout = 5)
                except:
                    continue
                return
        threading.Thread(target = shutdown_server).start() # worker
        threading.Thread(target = shutdown_worker).start()
        # Wait for both processes to finish
        process1.join()
        process2.join()
    print('done')
if __name__ == "__main__":
    main()
    