import logging

# Create a logger
logger = logging.getLogger('my_task_dispatcher')
logger.setLevel(logging.DEBUG)

# # Create a console handler and set its format
console_handler = logging.StreamHandler()
#formatter = logging.Formatter("%(levelname)s - %(message)s")
formatter = logging.Formatter("%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
fh = logging.FileHandler('my_task_dispatcher.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.debug('start loading module task_dispatcher.py')

import socket
import  queue
import OrderedDictQueue
from threading import Thread, Lock, Event
import time
import inspect
from typing import List, Dict
import datetime

from contextlib import contextmanager
debug = True
stop_event = Event()
ODictQueue: OrderedDictQueue.OrderedDictQueue = OrderedDictQueue.OrderedDictQueue  # optimized for change in order, pop first often
dictQueue:OrderedDictQueue.dictQueue = OrderedDictQueue.dictQueue        #optimised for key pop

# TO-DO
"""
1. on task spoilt for short
    put task back on to do list
2. on task spoilt for too long,
    remove task and log
3. On  successfully returned finishted task that last for too long:
    drop task, log
"""


from abc import ABC, abstractmethod
import time

class TimerContext:
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        if debug:
            self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if debug:
            elapsed_time = time.time() - self.start_time
            print(f"{self.label} took {elapsed_time:.6f} seconds.")


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
def require_either_method(methods):
    def decorator(cls):
        if all(map(hasattr, [cls] * len(methods),  methods)):
            raise NotImplementedError(f"Subclasses must implement 1 method in {methods}")
        return cls
    return decorator
Require_Reduce_or_Watcher_cls_decorator = require_either_method(["_reduce", 'reduce_watcher'])
def require_either_method(methods):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if all(map(hasattr, [args[0]] * len(methods),  methods)):
                raise NotImplementedError(f"Class must implement either at least 1 function in {methods}")
            return func(*args, **kwargs)
        return wrapper
    return decorator
#@Require_Reduce_or_Watcher_cls_decorator()


class Worker(ABC):
    def __init__(self, id, worker_manager:"Worker_Sorter", pending_task_limit = 24  , *arg, index_in_sorter = None, **kwarg):
        # initi connection
        # local = True, remote = None, 
        self.map_called = 0
        self.reduce_called = 0
        self.worker_manager = worker_manager
        self.task_manager = self.worker_manager.task_manager
        self.lock = Lock()
        self.index_in_sorter = index_in_sorter
        self.pending_task_limit = pending_task_limit# min(pending_task_limit, self.task_manager.output_minibatch_size)
        self.dispatched_task_limit = 12 # must be greater than or equal to minibatch limit
        self.id = id
        self.queue_pending = self.task_manager.worker_pending[self.id]
        self.queue_dispatched = self.task_manager.worker_dispatched[self.id]
        with self.queue_dispatched.not_full:
            self.queue_dispatched.not_full.notify()
        with self.queue_pending.not_full:
            self.queue_pending.not_full.notify()
         # if set False, must implement mechanism to call _reduce
        # such as calling reduce at the end of dispatch
        
        # to subclass below
        
        
        # TO-DO initialize remote connection
    def get_next_task(self):
        return self.queue_pending._first()
    def __len__(self): # number of total task
        return len(self.queue_pending) + len(self.queue_dispatched)
    def __lt__(self, other):
        return len(self) < len(other)
        
    def __getitem__(self):
        pass
    def start(self):
        def to_th_map():
            while True:
                try:
                    self.map()                        
                except Exception as e:
                    print(e)
                    raise
                

        th_map = Thread(target = to_th_map, args = [], name=f'{self.id} worker map')
        th_map.start()
        def to_th_reduce():
            while True:
                try:
                    self._map_result_watcher()
                except Exception as e:
                    print(e)
                    raise
        th_map_result_watch = Thread(target = to_th_reduce, args = [],name=f'{self.id} worker watcher')
        th_map_result_watch.start()
    @abstractmethod
    def dispatch(self):
        pass
    @abstractmethod
    def prepare_for_dispatch(self):
        pass
    
    
    def map(self, *arg, **kwarg):
        """
        map: holds queue pending, wait for queue_dispatched
        reduce: holds queue_dispatched
        """
        self.map_called += 1
        logger.debug(f"Worker{self.id}.map called {self.map_called}")
        logger.debug(f"Worker{self.id}.map acquiring queue_pending.mutex")
        with self.queue_pending.mutex:
            logger.debug(f"Worker{self.id}.map acquired queue_pending.mutex")
            if len(self.queue_pending) <= 0:
                #with TimerContext("Worker map"):
                    logger.debug(f"Worker{self.id}.map waiting queue_pending.not_empty")
                    self.queue_pending.not_empty.wait()
                    logger.debug(f"Worker{self.id}.map waited queue_pending.not_empty")
            
            task_info = self.queue_pending._popleft()
            if len(self.queue_pending) > 0:
                self.queue_pending.not_empty.notify()
                self.queue_pending.not_full.notify()
            logger.debug(f"Worker{self.id}.map queue_pending notify")
        
        if task_info is not None:
            data_for_dispatch = self.prepare_for_dispatch(task_info)
            logger.debug(f"Worker{self.id}.map acquring queue_dispatched.mutex")
            with self.queue_dispatched.mutex:
                logger.debug(f"Worker{self.id}.map acqured queue_dispatched.mutex")
                while len(self.queue_dispatched) >= self.dispatched_task_limit:
                    logger.debug(f"Worker{self.id}.map waiting queue_dispatched.not_full")
                    self.queue_dispatched.not_full.wait()
                    logger.debug(f"Worker{self.id}.map waited queue_dispatched.not_full")


                self.dispatch(data_for_dispatch)
                logger.debug(f"Worker{self.id}.map returned from dispatch")
                self.queue_dispatched._put(task_info[0], task_info[1]) # (time, id), user data
                logger.debug(f"Worker{self.id}.map dispatched_task_limit notifying")
                if len(self.queue_dispatched) < self.dispatched_task_limit:
                    self.queue_dispatched.not_full.notify()
                self.queue_dispatched.not_empty.notify()
                logger.debug(f"Worker{self.id}.map dispatched_task_limit notified")
        else:
            logger.warning("empty still passed not_empty condition")
            # task_info in format ((assignment_time, task_id), task data )
        

                # th_dispatch = Thread(target = self.dispatch, args = [data_for_dispatch],name=f'{self.id} worker dispatch')
                # th_dispatch.start()

            #self.queue_pending._popleft()
            
            # th_dispatch.then(self._map_result_watcher, kwargs = {'task_info': task_info}) \
            #     .then(self._reduce, kwargs = {"task_info": task_info})
        
        # dispatch result inserted to _reduce_watcher
        #self.reduce(task_info, result)
    
    def reduce (self,dispatch_result= None, task_info = None, map_result = None):
        """
        override this method to implement reduce behavour, default send back to task result queue
        """
        print(f"last step: {task_info} \nresults received with result type={type(map_result)}")
        
    
    def _map_result_watcher(self, dispatch_result, task_info = None, **kwarg):
        "watch_event_for_reduce, get map result and call _reduce at the end"
        map_result = None
        
        return map_result
    
    
    def _reduce(self, map_result, task_info,  *args, **kwargs  ): 
        self.reduce_called+=1
        logger.debug(f"Worker{self.id} reduce called {self.reduce_called}")
        logger.debug(f"Worker{self.id}._reduce acquiring queue_dispatched.mutex")
        with self.queue_dispatched.mutex:
            logger.debug(f"Worker{self.id}._reduce acquired queue_dispatched.mutex")
            if task_info in self.queue_dispatched.queue:
                task_id = task_info[1]
                
                # MAY NOT NEED NOTIFY
                self.queue_dispatched.queue.pop(task_info)
                logger.debug(f"Worker{self.id}._reduce queue_dispatched notify")
                self.queue_dispatched.not_full.notify()
                if len(self.queue_dispatched.queue) > 0:
                    self.queue_dispatched.not_empty.notify()
                logger.debug(f"Worker{self.id}._reduce update sort")
                self.worker_manager.update_sort(index_in_sorter=self.index_in_sorter, change = -1)
                logger.debug(f"Worker{self.id}._reduce update sorted")
                self.reduce(task_info ,dispatch_result = map_result)
                logger.debug(f"Worker{self.id}._reduce reduced")
            else:
                print(f"late arrival, drop Task info={task_info}")
                
                "most likely task is running remotely"
                
                "task not exist, maybe reduced by other worker, or this is a retry but the original task"
                "completed when retry task is pending, i.e. original may race with current, when retry function implemented"
                pass
        logger.debug("_reduce waiting q_task_completed.not_full")
        with self.task_manager.q_task_completed.not_full:
            logger.debug(f"Worker{self.id}._reduce acquired q_task_completed.not_full condition")
            if self.task_manager.q_task_completed._qsize() >= self.task_manager.q_task_completed_limit:
                logger.debug(f"Worker{self.id}._reduce q_task_completed is full")
                self.task_manager.q_task_completed.not_full.wait()
            logger.debug(f"Worker{self.id}._reduce acquired q_task_completed.not_full")
            record = (task_info, map_result)
            self.task_manager.q_task_completed._put(record)
            self.task_manager.q_task_completed.not_empty.notify()
        logger.debug(f"Worker{self.id}._reduce finish")
    def add_task(self, id, data, *arg, priority = None):
        # by default, first in should be prioritised such that redoing old task
        # will have highest priority
        # bug here when stoped it will return wrong !!!!!!!!!!!
        # in lock: q_task_outstanding
        if priority is None:
            priority = time.time()
        logger.debug(f"Worker{self.id}.add_task acquiring queue_pending.mutex")
        with self.queue_pending.not_full:
            logger.debug(f"Worker{self.id}.add_task acquired queue_pending.mutex")
            if len(self.queue_pending) >= self.pending_task_limit:
                logger.debug(f"Worker{self.id}.add_task acquired queue_pending.mutex")
                self.queue_pending.not_full.wait()
            self.queue_pending._put((priority,id), data)
            logger.debug(f"Worker{self.id}.add_task before notify")
            self.queue_pending.not_empty.notify()
            task_assignment[id] = {
                            'task_data': data, 
                            "assigned_to" : self.id} # worker id
            self.worker_manager.update_sort(index_in_sorter=0, change = 1)
        logger.debug(f"Worker{self.id}.add_task finish")
            
    def add_task2 (self, *arg, priority = None):
        if priority is None:
            priority = time.time()
        logger.debug(f"Worker{self.id}.add_task2 acquiring queue_pending.mutex")
        with self.queue_pending.not_full:
            logger.debug(f"Worker{self.id}.add_task2 has acquried queue_pending.mutex")
            if self.queue_pending._qsize() > self.queue_pending.maxsize:
                logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_full")
                self.queue_pending.not_full.wait()
                logger.debug(f"Worker{self.id}.add_task2 queue_pending is not full")
            logger.debug(f"Worker{self.id}.add_task2 acquiring q_task_outstanding.not_empty")
            with self.task_manager.q_task_outstanding.not_empty:
                logger.debug(f"Worker{self.id}.add_task2 has acquired q_task_outstanding.not_empty")
                if len(self.task_manager.q_task_outstanding) <= 0:
                    #with TimerContext("Task Work Man Asgn "):
                        logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_empty")
                        self.task_manager.q_task_outstanding.not_empty.wait()
                        logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_empty")
                packed = self.task_manager.q_task_outstanding._first()
                if packed is None:
                    logger.warning("empty but still has not_empty, work number tracker has error")
                    
                else:
                    task_id, data = packed
                    self.queue_pending._put((priority,task_id), data)
                    self.task_manager.q_task_outstanding._popleft()
                    
                    self.queue_pending.not_empty.notify()
                    task_assignment[id] = {
                            'task_data': data, 
                            "assigned_to" : self.id} # worker id
                    self.worker_manager.update_sort(index_in_sorter=0, change = 1)
                    
                    if len(self.task_manager.q_task_outstanding) > 0:
                        self.task_manager.q_task_outstanding.not_empty.notify()
                    if self.queue_pending._qsize() > self.queue_pending.maxsize:
                        pass
                    else:
                        self.task_manager.q_task_outstanding.not_full.notify()
                        
                    logger.debug(f"Worker{self.id}.add_task2 leaving")
class Worker_Sorter():
    """
    worker are created here,
    tasks get from elsewhere
    """
    worker_list : List[Worker] = []
    def __init__(self, sorting_algo, task_manager:"Task_Worker_Manager", worker_factory=Worker ):
        self.sorting_algo = sorting_algo
        self.worker_factory = worker_factory
        
        self.worker_list :List[Worker] = [] 
        self.worker_by_id : Dict[int,Worker] = {}
        self.new_worker_id = 0
        
        self.worker_list_lock = Lock()
        # alias
        self.task_manager = task_manager
        self.task_retry_timeout = self.task_manager.task_retry_timeout
        self.q_task_completed = self.task_manager.q_task_completed
        
        for config in task_manager.worker_config:
            self.add_worker(config)
            # self.worker_list.append(self.worker_factory(worker_manager = self,
            #     id = self.new_worker_id, 
            #     is_local = config['location'] == 'local', 
            #     remote_info = None if config['location'] == 'local' else config['location'],
            #     index_in_sorter = len(self.worker_list)
            # ))
            # self.worker_list_by_id[self.new_worker_id] = self.worker_list[-1]
            # self.new_worker_id += 1
        
    def update_sort(self, index_in_sorter: int, change : int):
        from heapq import _siftdown, _siftup
        if change > 0:
            _siftup(self.worker_list, index_in_sorter)
        else:
            _siftdown(self.worker_list, index_in_sorter, len(self.worker_list)-1)
    def get_worker(self, id = None, index_in_sorter = None):
        # get freest worker if both id and index_in_sorter is None
        return self.worker_list[0]
    def add_worker(self, config):
        with self.worker_list_lock:
            self.worker_list.append(self.worker_factory(
                worker_manager = self,
                id = self.new_worker_id, 
                is_local = config['location'] == 'local', 
                remote_info = None if config['location'] == 'local' else config['location'],
                index_in_sorter = len(self.worker_list)
            ))
            self.worker_by_id[self.new_worker_id] = self.worker_list[-1]
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
    def get_freest_worker(self):
        if self.sorting_algo == 'heapq':
            free_worker = self.worker_list[0]
        else:
            raise NotImplementedError
        return free_worker        
    def add_task(self, task_id, data, *arg, worker_id = None):
        # brutforce search task through each worker, not recommended
        # for high performance
        # in lock: q_task_outstanding
        logger.debug("Worker_sorter.add_task finding freest worker")
        if worker_id is None:
            free_worker = self.get_freest_worker()
        else:
            free_worker = self.worker_by_id[worker_id]
        logger.debug("Worker_sorter.add_task freest worker found")
        free_worker.add_task( task_id, data)
            
        task_assignment[task_id] = {
                         'task_data': data, 
                         "assigned_to" : free_worker.id} # worker id
        self.update_sort(index_in_sorter=0, change = 1)
    def add_task2(self, *arg, worker_id = None):
        logger.debug("Worker_sorter.add_task2 finding freest worker")
        if worker_id is None:
            free_worker = self.get_freest_worker()
        else:
            free_worker = self.worker_by_id[worker_id]
        logger.debug("Worker_sorter.add_task2 freest worker found")
        free_worker.add_task2()
            
        
        
    def pop_task(self, task_id, worker_id = None):
        if worker_id is None:
            worker_id = task_assignment[task_id]['assigned_to']
    

            
    def check_workers_timeout_retry(self):
        while True:
            for worker in self.worker_by_id.values():
                with worker.queue_dispatched.not_empty:
                    if worker.queue_dispatched._qsize() <= 0:
                        worker.queue_dispatched.not_empty.wait()
                    while worker.queue_dispatched._qsize() > 0:
                        task_info, task_data = worker.queue_dispatched._first()
                        timenow = time.time()
                        if timenow - task_info[0] > self.task_manager.task_retry_timeout:
                            
                            worker.queue_dispatched.queue.pop(task_info)
                            self.update_sort(index_in_sorter=worker.index_in_sorter, change = -1)
                            """
# issue:                         assign holds: queue_pending ,  waiting for : q_task_outstanding
                                    map holds: queue_pending        waiting for : queue_dispatched
            check_workers_timeout_retry holds: queue_dispatched     waiting for : q_task_outstanding
            ding ding: dead lock cycle
                            """                        
                            self.task_manager.dispatch(task_data, ignore_size_check = True) # use outstanding
                            worker.queue_dispatched.not_full.notify()
                            if worker.queue_dispatched._qsize() > 0:
                                worker.queue_dispatched.not_empty.notify()
                        else:
                            worker.queue_dispatched.not_empty.notify()
                            break # to next worker
                        
            
            time.sleep(max(self.task_retry_timeout, 0.1))            

        
    def start(self):
        with self.worker_list_lock:
            for id, w in self.worker_by_id.items():
                w.start()
        if self.task_retry_timeout == float('inf') or self.task_retry_timeout == 0:
            pass
        else:
            th_retry_watch = Thread(target= self.check_workers_timeout_retry, name = 'worker timeout watcher')
            th_retry_watch.start()
            self.th_retry_watch = th_retry_watch

class Task_Worker_Manager():
    """
    Entry point of object
    tasks are created here,
    worker get from elsewhere
    """
    def __init__(self, worker_sorter_algo = 'heapq', worker_sorter_factory = Worker_Sorter, worker_factory = Worker, worker_config = None,
                 output_minibatch_size = 1, task_retry_timeout = 3, task_stale_timeout = 6 ):
        if worker_config is not None:
            self.n_worker = len(worker_config)
        self.assign_task_called = 0 
        self.max_outstanding = 100
        self.output_minibatch_size = output_minibatch_size
        self.task_retry_timeout = task_retry_timeout  #float("inf")
        self.task_stale_timeout = task_stale_timeout # float("inf")
        self.worker_sorter_factory = worker_sorter_factory
        if worker_config is None:
            worker_config = []
        self.worker_config = worker_config
        self.q_task_outstanding: OrderedDictQueue.OrderedDictQueue = ODictQueue(maxsize=50)  # pop first often, prioritize unoften, error re-enter from workign on
        self.q_task_completed = queue.PriorityQueue() # frames order is important to allow smooth streaming
        self.worker_pending : Dict[int, OrderedDictQueue.dictQueue] = {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_dispatched : Dict[int, OrderedDictQueue.dictQueue] = {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_sorter_algo=worker_sorter_algo # [heapq, bincount]
        self.q_task_completed_limit = 1000
        self.th_assign = None
        self.th_clean_up = None
        self.stop = False
        self.watermark = time.time()
        self.water_mark_lookback = 60
        self.watermark_check = False
        self._worker_sorter = self.worker_sorter_factory(sorting_algo = 'heapq', task_manager = self, worker_factory=worker_factory)
        self.worker_factory = self._worker_sorter.worker_factory
        self.init_locks()
    def init_locks(self):
        with self.q_task_outstanding.not_full:
            self.q_task_outstanding.not_full.notify()
        with self.q_task_completed.not_full:
            self.q_task_completed.not_full.notify()
        for id, dq in self.worker_pending.items():
            with dq.not_full:
                dq.not_full.notify()
        for id, dq in self.worker_dispatched.items():
            with dq.not_full:
                dq.not_full.notify()

    def start(self):
        th = Thread(target = self.assign_task, args = [], name='task work manager assign task')
        th.start()
        self.th_assign_task = th
        self._worker_sorter.start()
        self.th_assign = th
        th = Thread(target = self.on_task_complete, args = [], name='task work manager task complete')
        th.start()
        self.th_clean_up = th
        

    def assign_task(self):
        """
        assign task from outstanding to worker
        """
        
        while not self.stop:
            try:
                self.assign_task_called += 1
                logger.debug(f"Task_Worker_Manager.assign_task called {self.assign_task_called}")
                with self.q_task_outstanding.not_empty:
                    
                    if len(self.q_task_outstanding) <= 0:
                #        with TimerContext("Task Work Man Asgn "):
                            self.q_task_outstanding.not_empty.wait()
                    
                    packed = self.q_task_outstanding._popleft()
                    if packed is None:
                        logger.warning("pop should not happen when empty, tracker has error")
                    if len(self.q_task_outstanding) > 0:
                        self.q_task_outstanding.not_empty.notify()
                task_id, data = packed
                self._worker_sorter.add_task(task_id, data)    

                    
            except Exception as e :
                print(e)
                raise
            #time.sleep(next_sleep)
    def assign_task2(self):
        while not self.stop:
            
            self._worker_sorter.add_task2()
                
        pass
    def dispatch(self, data, ignore_size_check = False, *arg, **kwarg):
        """
        task related information formed here
        to be called by as data entry point
        input task data in arg

        ignore_size_check only used when adopting the retrying function

        # TO-DO, when full, eject latest, dispatch other worker
        """
        logger.debug("Task_manager.dispatch start")
        task_id = str(uuid.uuid1())
        logger.debug("Task_manager.dispatch acquiring q_task_outstanding.mutex")
        with self.q_task_outstanding.mutex:
            logger.debug("Task_manager.dispatch has acquired q_task_outstanding.mutex")
            if (len(self.q_task_outstanding) >= self.max_outstanding) and not ignore_size_check:
                self.q_task_outstanding.not_full.wait()
                
                
            self.q_task_outstanding._put(task_id, data)
            
            self.q_task_outstanding.not_empty.notify()
            
    def print_all_queues(self):
        print_all_queues(self)
    def on_task_complete(self):
        #function that operate on task completed
        logger.debug("Task_manageron.on_task_complete start")
        output_minibatch = [()] * self.output_minibatch_size
        complete_count = 0
        empty_waited_count = 0
        while True:
            
            try:
                logger.debug("Task_manager.on_task_complete start waiting for minibatch")
                with self.q_task_completed.not_empty:
                    while self.q_task_completed._qsize() < self.output_minibatch_size:
                        
                        logger.debug("Task_manager.on_task_complete wait more record for minibatch")
                        self.q_task_completed.not_empty.wait()
                        empty_waited_count += 1
                        logger.debug(f"Task_manager.on_task_complete q size={self.q_task_completed._qsize()}")
                        logger.debug(f"Task_manager.on_task_complete empty_waited_count={empty_waited_count}")
                    if self.q_task_completed._qsize() >= self.output_minibatch_size:
                        logger.debug("Task_manager.on_task_complete waited minibatch")
                        for i in range(self.output_minibatch_size):
                            logger.debug(f'Task_manageron.on_task_complete minibatch record {i}')
                            completed = self.q_task_completed._get()
                            self.q_task_completed.not_full.notify()
                            if self.q_task_completed._qsize() > 0:
                                self.q_task_completed.not_empty.notify()
                            task_time = completed[0]
                            if time.time() - task_time[0] > self.task_stale_timeout:
                                logger.debug(f"task {completed[0]} stale")
                                continue
                            else:
                                logger.debug(f"task {completed[0]} complete")
                            complete_count += 1
                            logger.debug(f'complete cnt {complete_count}')
                            cnt_fresh = 0
                            if completed[0][0] > self.watermark-self.water_mark_lookback:
                                
                                self.watermark = completed[0][0]
                                output_minibatch[cnt_fresh] = completed
                                cnt_fresh += 1
                                logger.debug(f'fresh cnt {cnt_fresh}')
                        def send_output(fresh_output_minibatch):
                            print(f"len fresh output = {len(fresh_output_minibatch)}")
                        send_output(output_minibatch[:cnt_fresh])
                        logger.debug(f'fresh cnt {cnt_fresh} sent')
                        output_minibatch = [()] * self.output_minibatch_size
                            
                    # if self.task_retry_timeout == float('inf'):
                    #     time.sleep(1)
                    # else:
                    #     time.sleep(max(0.1, self.task_retry_timeout/10))
            except Exception as e:
                print(e)
                pass

from pprint import pprint
def print_all_queues(my_task_worker_manager: Task_Worker_Manager = None):
    
    print("Task_Worker_Manager:")
    print("  q_task_outstanding", len(my_task_worker_manager.q_task_outstanding))
    print("  q_task_completed", my_task_worker_manager.q_task_completed._qsize())
    print("Worker_Sorter:")
    print("workers:", my_task_worker_manager._worker_sorter.worker_by_id)
    print("workers task:", [len(worker) for id, worker in my_task_worker_manager._worker_sorter.worker_by_id.items()])
    print("workers heap:", [len(worker) for worker in my_task_worker_manager._worker_sorter.worker_list])
    for w in my_task_worker_manager._worker_sorter.worker_list:
        pprint(w)
        pprint(w.queue_pending.queue)
        print("pending_task_limit", w.pending_task_limit)
        print("pending_task", len(w.queue_pending.queue))
        
        print("dispatched_task_limit", w.dispatched_task_limit)
        print("dispatched_task", len(w.queue_dispatched.queue))
        pprint(w.queue_dispatched.queue)
if __name__ == '__main__':
    my_task_worker_manager = Task_Worker_Manager()
    #my_task_worker_manager.start()
    import numpy as np
    task_data = enumerate([np.array(range(20)).reshape([4,5])])
    cnt = 0
    while True:
        try:
            if debug:
                print("task_dispatcher.__main__")
            frame_data = {"frame_number": cnt, "face": np.random.rand(96,96,3), 'mel': np.random.rand(80,16)}
            my_task_worker_manager.dispatch(frame_data)
            print_all_queues(my_task_worker_manager)
        except Exception as e:
            print(e)
            raise
            
        #time.sleep(0.1)


