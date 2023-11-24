import time
from sqlitedict import SqliteDict
from multiprocessing import Process
def perform_operations(dv):
    db = SqliteDict(dv, autocommit=True)
    iterations = 1000

    start_time = time.time()

    for i in range(iterations):
        # Write operation
        db['my_integer'] = i  # Writing an integer to the dictionary

        # Read operation
        my_integer = db['my_integer']  # Reading the integer from the dictionary

    end_time = time.time()
    execution_time = end_time - start_time

    # Print the results
    print("Iterations:", iterations)
    print("Total Execution Time:", execution_time)
    print("Average Time per Operation:", execution_time / iterations)

    # Close the database connection
    db.close()

if __name__ == "__main__":
    

    # Function to perform read and write operations
    


    # Create two processes
    process1 = Process(target=perform_operations, args=("database1.sqlite",))
    process2 = Process(target=perform_operations, args=("database2.sqlite",))

    # Start the processes
    process1.start()
    process2.start()

    # Wait for the processes to finish
    process1.join()
    process2.join()