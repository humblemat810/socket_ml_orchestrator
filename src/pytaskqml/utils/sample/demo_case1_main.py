import multiprocessing
import subprocess

def run_file1():
    # Define the arguments for the first Python file
    args_file1 = ["python", r"demo_case1_wordcount_worker.py", "--port", "12345", "--management-port", "22345", "--config", 'worker.ini']
    subprocess.call(args_file1)

def run_file2():
    # Define the arguments for the second Python file
    args_file2 = ["python", r"demo_case1_dispatcher.py", "--management-port", "18000", "--config", 'dispatcher.ini']
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
if __name__ == "__main__":
    import os, pathlib
    with ChangeDirectory(str(pathlib.Path(__file__).parent)):
        

        # Create two separate processes for running the files
        process1 = multiprocessing.Process(target=run_file1)
        process2 = multiprocessing.Process(target=run_file2)

        # Start both processes
        process1.start()
        process2.start()
        import time
        time.sleep(5)
        import requests
        from threading import Thread
        Thread(target = requests.get, args = ['http://localhost:22345/shutdown']).start() # worker
        Thread(target = requests.get, args = ['http://localhost:18000/shutdown']).start()
        # Wait for both processes to finish
        process1.join()
        process2.join()
    print('done')