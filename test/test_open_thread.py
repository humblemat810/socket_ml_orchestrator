import threading

# Create an event object
event = threading.Event()

# Shared flag variable to signal termination
terminate_flag = False

# Define a worker function that waits for the event
def worker():
    while not terminate_flag:
        print("Worker thread waiting for event...")
        event.wait()  # Wait for the event to be set
        event.clear()  # Clear the event for future waits
        print("Worker thread un-waited!")

# Create and start the worker threads
threads = []
for _ in range(3):
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

# Set the event to un-wait the worker threads
event.set()

# Wait for some time
print("Main thread sleeping...")
threading.Event().wait(2)  # Wait for 2 seconds

# Set the terminate flag to signal threads to exit
terminate_flag = True

# Set the event to un-wait any threads still waiting
event.set()

# Wait for the worker threads to finish
for thread in threads:
    thread.join()

print("Main thread finished.")