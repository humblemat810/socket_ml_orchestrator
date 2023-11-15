import os
import socket
import  queue
import OrderedDictQueue
from threading import Thread, Lock, Event
import time
import inspect
from typing import List
from client_ml import ML_socket_client
stop_event = Event()
ODictQueue = OrderedDictQueue.OrderedDictQueue  # optimized for change in order, pop first often
dictQueue = OrderedDictQueue.dictQueue        #optimised for key pop

def abstract_method(original_func):
    def wrapper(*args, **kwargs):
        # Check if the function is being called from a subclass
        if not issubclass(args[0].__class__, original_func.__self__.__class__):
            raise RuntimeError(f"{original_func.__name__} must be called from a subclass.")

        return original_func(*args, **kwargs)

    return wrapper

def is_socket_connected(sock):
    try:
        # Check the socket connection state
        sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        return True
    except socket.error:
        return False


"""
pending should be same as dispatched if locally run single thread
"""
import uuid
from typing import Optional
task_info_template = {"task_id": str, 
                         'task_data': object, 
                         "task_type": str, 
                         "assigned_to" : Optional[str]} # worker id
task_assignment = ODictQueue()
from thennable_thread import thennable_thread
# class ThreadWithReturnValue(Thread):
#     def __init__(self, group=None, target=None, name=None,
#                  args=(), kwargs={}, Verbose=None):
#         Thread.__init__(self, group, target, name, args, kwargs, Verbose)
#         self._return = None
#     def run(self):
#         if self._Thread__target is not None:
#             self._return = self._Thread__target(*self._Thread__args,
#                                                 **self._Thread__kwargs)
#     def join(self):
#         Thread.join(self)
#         return self._return
    


class Worker():
    def __init__(self, id, worker_manager:"Worker_Sorter"  , *arg, index_in_sorter = None, **kwarg):
        # initi connection
        # local = True, remote = None, 
        self.worker_manager = worker_manager
        self.task_manager = self.worker_manager.task_manager
        self.lock = Lock()
        self.index_in_sorter = index_in_sorter
        
        self.id = id
        self.queue_pending = self.task_manager.worker_pending[self.id]
        self.queue_dispatched = self.task_manager.worker_dispatched[self.id]
        # to subclass below
        
        
        # TO-DO initialize remote connection
    def get_next_task(self):
        return self.queue_pending.first()
    def __len__(self): # number of total task
        return len(self.queue_pending) + len(self.queue_dispatched)
    def __lt__(self, other):
        return len(self) < len(other)
        
    def __getitem__(self):
        pass
    @abstract_method
    def map(self, *arg, **kwarg):
        with self.queue_pending.not_empty:
            task_info = self.queue_pending._first()
            data_for_dispatch = self.prepare_for_dispatch(task_info)
            with self.queue_dispatched.not_full:
                thm = thennable_thread(target = self.dispatch, args = [data_for_dispatch])
                thm.start()
                
                #result = self.dispatch(data=data_for_dispatch)
                self.queue_dispatched._put(task_info)
            self.queue_pending.popleft()
            thm.then(target = self.reduce, args = [task_info])
        #self.reduce(task_info, result)
    @abstract_method
    def reduce (self, task_info,result):
        """
        override this method to implement reduce behavour, default send back to task result queue
        """
        with self.queue_dispatched.not_empty:
            if task_info.task_id in self.queue_dispatched.not_empty.get():
                a = self.queue_dispatched.not_empty.get(task_info.task_id)
                with self.task_manager.q_task_completed.not_full:
                    self.task_manager.q_task_completed.put(task_info.priority, task_info, a, result)
                    self.queue_dispatched.pop(task_info.task_id)

    @abstract_method
    def _reduce(self, th_last_step: thennable_thread, task_info):
        results = th_last_step.join()
        self.reduce(task_info , results)
    def add_task(self, priority, *arg):
        # by default, first in should be prioritised such that redoing old task
        # will have highest priority
        priority = time.time()
        with self.queue_pending.not_full:
            self.queue_pending._put(priority, arg)
class Worker_Sorter():
    """
    worker are created here,
    tasks get from elsewhere
    """
    worker_list : List(Worker)
    def __init__(self, sorting_algo, task_manager:"Task_Worker_Manager", worker_factory=Worker, ):
        self.sorting_algo = sorting_algo
        self.worker_factory = worker_factory
        
        self.worker_list = [] 
        self.new_worker_id = 0
        
        # alias
        self.task_manager = task_manager
        self.q_task_completed = self.task_manager.q_task_completed
        
        for config in task_manager.worker_config:
            self.worker_list.append(self.worker_factory(worker_manager = self,
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
        self.worker_list.append(Worker(
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
    def add_task(self, task_id, *arg, worker_id = None):
        # brutforce search task through each worker, not recommended
        # for high performance

        if worker_id is None:
            free_worker = self.worker_list[0]
        else:
            free_worker = self.workers_by_id[worker_id]
        free_worker.add_task( task_id, *arg)
        task_assignment[task_id] = {
                         'task_data': arg, 
                         "assigned_to" : free_worker.id} # worker id
        self._siftup(pos = 0)
    def pop_task(self, task_id, worker_id = None):
        if worker_id is None:
            worker_id = task_assignment[task_id]['assigned_to']
    def on_task_complete(self):
        #function that operate on task completed
        while True:
            with self.q_task_completed:
                pass
    def check_timeout_task(self):
        pass

    def check_worker_alive(self):
        pass

    def start(self):
        th = Thread(target = self.on_task_complete)
        th.start()
        self.th = th

class Task_Worker_Manager():
    """
    Entry point of object
    tasks are created here,
    worker get from elsewhere
    """
    def __init__(self, worker_sorter_name = 'heapq', worker_sorter_factory = Worker_Sorter):
        self.n_worker = 3
        self.worker_sorter_factory = worker_sorter_factory
        self.worker_config = [{"location": "local"}, {"location": ("localhost", 12345)}, {"location": ("192.168.1.2", 12345)}]
        self.q_task_outstanding = ODictQueue(maxsize=50)  # pop first often, prioritize unoften, error re-enter from workign on
        self.q_task_completed = queue.PriorityQueue() # frames order is important to allow smooth streaming
        self.worker_pending = {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_dispatched = {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_sorter_name=worker_sorter_name # [heapq, bincount]
        self._worker_sorter = self.worker_sorter_factory(sorting_algo = 'heapq', task_manager = self)
        self.th = None
        self.stop = False
    def start(self):
        th = Thread(target = self.assign_task)
        th.start()
        self.th = th

    def assign_task(self):
        while not self.stop:
            with self.q_task_outstanding.not_empty:
                task_id, arg = self.q_task_outstanding.get()
                self._worker_sorter.add_task(task_id, *arg)
        
    def dispatch(self, *arg):
        """
        task related information formed here
        """
        task_id = uuid.uuid1()
        with self.q_task_outstanding.not_full:
            self.q_task_outstanding.put((task_id, arg))




if __name__ == '__main__':
    my_task_worker_manager = Task_Worker_Manager()
    my_task_worker_manager.start()
    import numpy as np
    task_data = enumerate([np.array(range(20)).reshape([4,5])])
    for frame_number, frame_data in task_data:
        my_task_worker_manager.dispatch(frame_number, frame_data)


