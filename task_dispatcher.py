import socket
import  queue
import OrderedDictQueue
from threading import Thread, Lock, Event
import time
from client_ml import ML_socket_client
stop_event = Event()
ODictQueue = OrderedDictQueue.OrderedDictQueue  # optimized for change in order, pop first often
dictQueue = OrderedDictQueue.dictQueue        #optimised for key pop
n_worker = 3
worker_config = [{"location": "local"}, {"location": ("localhost", '12345')}, {"location": ("192.168.1.2", '12345')}]
q_task_outstanding = ODictQueue(maxsize=50)  # pop first often, prioritize unoften, error re-enter from workign on
q_task_completed = queue.PriorityQueue() # frames order is important to allow smooth streaming
worker_pending = {id: dictQueue(maxsize=50) for id in range(n_worker)}
worker_dispatched = {id: dictQueue(maxsize=50) for id in range(n_worker)}
"""
pending should be same as dispatched if locally run single thread
"""
import uuid
from typing import Optional
task_info_template = {"task_id": str, 
                         'task_data': object, 
                         "task_type": str, 
                         "assigned_to" : Optional[str]} # worker id
task_assignment = {}
class all_queue_tracker():
    worker_pending=worker_pending
    worker_dispatched = worker_dispatched
    q_task_outstanding = q_task_outstanding
    q_task_completed = q_task_completed
class TaskResults():
    def __init__(self, task_id, frame_number, result):
        self.task_id = task_id
        self.frame_number = frame_number
        self.result = result
        
class worker():
    def __init__(self, id, local = True, remote = None, index_in_sorter = None):
        # initi connection
        self.lock = Lock()
        self.index_in_sorter = index_in_sorter
        self.local = local
        self.id = id
        self.queue_pending = all_queue_tracker.worker_pending[self.id]
        self.queue_dispatched = all_queue_tracker.worker_dispatched[self.id]
        if local:
            self.client_socket = None
        else:
            self.server_address = remote # eg example : ('localhost', 5000)
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(self.server_address)
        # TO-DO initialize remote connection
    def get_next_task(self):
        return self.queue_pending.first()
    def __len__(self): # number of total task
        return len(self.queue_pending) + len(self.queue_dispatched)
    def __lt__(self, other):
        return len(self) < len(other)
        
    def __getitem__(self):
        pass
    def work(self):
        #run on thread
        if self.local:
            def call_ml(data):
                pass
            #task_id, data = self.get_next_task()
            with self.queue_pending.not_empty:
                #with self.queue_dispatched.not_full:
                    frame_number, (task_id, data) = self.queue_pending._get()
                    result = call_ml(data=data)
                    # skip self.queue_dispatched
            q_task_completed.put(TaskResults(task_id, frame_number, result))
            # add data to local processing thread
            # return self remote call
            
        else:
            # add data to remote socket manager thread
            # send data frame to worker via socket
            pass
    def add_task(self, task_id, frame_number, task_data):
        with self.queue_pending.not_full:
            self.queue_pending._put(frame_number, (task_id, task_data))
            
# abstract type for bin count and heapq        
class worker_sorter():
    """
    worker are created here,
    tasks get from elsewhere
    """
    def __init__(self, sorting_algo):
        self.sorting_algo = sorting_algo
        self.worker_list = [] 
        self.new_worker_id = 0
        for config in worker_config:
            self.worker_list.append(worker(
                id = self.new_worker_id, 
                local = config['location'] == 'local', 
                remote = None if config['location'] == 'local' else config['location'],
                index_in_sorter = len(self.worker_list)
            ))
            self.new_worker_id += 1
        self.workers_by_id = {i.id:i for i in self.worker_list}
    def get_worker(self, id = None, index_in_sorter = None):
        # get freest worker if both id and index_in_sorter is None
        return self.worker_list[0]
    def add_worker(self, config):
        self.worker_list.append(worker(
            id = self.new_worker_id, 
            local = config['location'] == 'local', 
            remote = None if config['location'] == 'local' else config['location'],
            index_in_sorter = len(self.worker_list)
        ))
        self.workers_by_id[self.new_worker_id] = self.worker_list[-1]
        self.new_worker_id += 1
    def pop_worker(self):
        # pop freest worker and distribute workload to other workers
        raise NotImplementedError
        pass
    def _siftup(self, pos):
        heap = self.worker_list
        endpos = len(heap)
        startpos = pos
        newitem = heap[pos]
        # Bubble up the smaller child until hitting a leaf.
        childpos = 2*pos + 1    # leftmost child position
        while childpos < endpos:
            # Set childpos to index of smaller child.
            rightpos = childpos + 1
            if rightpos < endpos and not heap[childpos] < heap[rightpos]:
                childpos = rightpos
            # Move the smaller child up.
            index_in_sorter = heap[pos].index_in_sorter
            heap[pos] = heap[childpos]
            heap[pos].index_in_sorter = index_in_sorter
            pos = childpos
            childpos = 2*pos + 1
        # The leaf at pos is empty now.  Put newitem there, and bubble it up
        # to its final resting place (by sifting its parents down).
        index_in_sorter = heap[pos].index_in_sorter
        heap[pos] = newitem
        heap[pos].index_in_sorter = index_in_sorter
        self._siftdown(heap, startpos, pos)
    def _siftdown(self, startpos, pos):
        heap = self.worker_list
        newitem = heap[pos]
        # Follow the path to the root, moving parents down until finding a place
        # newitem fits.
        while pos > startpos:
            parentpos = (pos - 1) >> 1
            parent = heap[parentpos]
            if newitem < parent:
                index_in_sorter = heap[pos].index_in_sorter
                heap[pos] = parent
                heap[pos].index_in_sorter = index_in_sorter
                pos = parentpos
                continue
            break
        index_in_sorter = heap[pos].index_in_sorter
        heap[pos] = newitem
        heap[pos].index_in_sorter = index_in_sorter
    def get_task(self, task_id):
        # brutforce search task through each worker, not recommended
        # for high performance
        pass
    def add_task(self, task_id,frame_number, worker_id = None, task_data = None):
        # brutforce search task through each worker, not recommended
        # for high performance

        if worker_id is None:
            free_worker = self.worker_list[0]
        else:
            free_worker = self.workers_by_id[worker_id]
        free_worker.add_task( task_id, frame_number, task_data)
        task_assignment[task_id] = {
                         'task_data': task_data, 
                         "frame_number" : frame_number,
                         "assigned_to" : free_worker.id} # worker id
        self._siftup(pos = 0)
    def pop_task(self, task_id, worker_id = None):
        if worker_id is None:
            worker_id = task_assignment[task_id]['assigned_to']
        # brutforce search task through each worker, not recommended
        # for high performance


class task_worker_manager():
    """
    tasks are created here,
    worker get from elsewhere
    """
    def __init__(self, worker_sorter_name = 'heapq'):
        self.worker_sorter_name=worker_sorter_name # [heapq, bincount]
        self._worker_sorter = worker_sorter(sorting_algo = 'heapq')
        self.th = None
        self.stop = False
    def start(self):
        th = Thread(target = self.assign_task)
        th.start()
        self.th = th

    def assign_task(self):
        while not self.stop:
            with q_task_outstanding.not_empty:
                task_id, frame_number, task_data = q_task_outstanding.get()
                worker_sorter.add_task(task_id, frame_number, task_data)
        
def dispatch(frame_number, task_data):
    """
    task related information formed here
    """
    task_id = uuid.uuid1()
    with q_task_outstanding.not_full():
        q_task_outstanding.put(task_id, frame_number, task_data)
    




if __name__ == '__main__':
    my_task_worker_manager = task_worker_manager()
    my_task_worker_manager.start()
    import numpy as np
    task_data = [np.array(range(20)).reshape([4,5])]
    for frame_number, frame_data in task_data:
        dispatch(frame_number, frame_data)


