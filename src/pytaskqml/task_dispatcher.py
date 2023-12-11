from pytaskqml.utils.reconnecting_socket import ReconnectingSocket
import traceback
import logging
class MyNullHandler(logging.Handler):
    def emit(self, record):
        pass
    def handle(self, record):
        pass
    def createLock(self):
        self.lock = None
class worker_config(dict):
    def __init__(self, *arg, **kwarg):
        self.min_start_processing_length: int
        self.location: str
        __all__= ['min_start_processing_length', 'location']
        if len(arg) == 1:
            for field_name in __all__:
                setattr(self, field_name, arg[field_name]) 
# Remove the default StreamHandler from the root logger
logging.getLogger().handlers.clear()

# Create a custom NullHandler
null_handler = MyNullHandler()

# Add the NullHandler to the root logger
logging.getLogger().addHandler(null_handler)

import socket
import  queue
from .utils.OrderedDictQueue import OrderedDictQueue, dictQueue
import threading
import time
from typing import List, Dict
from http.server import BaseHTTPRequestHandler, HTTPServer

debug = True
stop_event = threading.Event()
ODictQueue: OrderedDictQueue = OrderedDictQueue  # optimized for change in order, pop first often


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



"""
pending should be same as dispatched if locally run single thread
"""
import uuid
from heapq import _siftdown, _siftup, heappop, heappush
from typing import Optional,Union
task_info_template = {"task_id": str, 
                         'task_data': object, 
                         "task_type": str, 
                         "assigned_to" : Optional[str]} # worker id
task_assignment = ODictQueue()

from typing import Any, Tuple

class task_info():
    def __init__(self):
        self.task_data : Any
        self.task_info : Tuple[float, uuid.UUID] # time, UUID] # TO-DO change to namedtuple

class Worker(ABC):
    def __init__(self, id, worker_manager:"Worker_Sorter", pending_task_limit = 24  ,
                 min_start_processing_length = None,
                 *arg, index_in_sorter = None, **kwarg):
        # initi connection
        # local = True, remote = None, 
        
        self.map_called = 0
        self.reduce_called = 0
        self.worker_manager = worker_manager
        self.task_manager = self.worker_manager.task_manager
        self.logger = self.worker_manager.task_manager.logger
        self.lock = threading.Lock()
        self.index_in_sorter = index_in_sorter
        self.pending_task_limit = pending_task_limit# min(pending_task_limit, self.task_manager.output_minibatch_size)
        self.dispatched_task_limit = 12 # must be greater than or equal to minibatch limit
        self.id = id
        self.th_map: threading.Thread
        if min_start_processing_length is None:
            min_start_processing_length = 4096
        self.min_start_processing_length = min_start_processing_length
        self.queue_pending = dictQueue(maxsize=50)
        self.worker_manager.worker_pending[self.id] = self.queue_pending
        self.queue_dispatched = dictQueue(maxsize=50)
        self.worker_manager.worker_dispatched[self.id] = self.queue_dispatched
        self.map_result_buffer = queue.Queue()
        self.stop_flag = threading.Event()
        self.graceful_stopped = threading.Event()
        self.worker_queue: Union[Worker_Sorter.worker_list, Worker_Sorter.removinged_worker_list]= self.worker_manager.worker_list
        with self.queue_dispatched.not_full:
            self.queue_dispatched.not_full.notify()
        with self.queue_pending.not_full:
            self.queue_pending.not_full.notify()
        def to_th_map():
            while not self.stop_flag.is_set():
                try:
                    self.map()                        
                except Exception as e:
                    print(e)
                    raise
            self.logger.info(f'worker {self.id} th_map loop finish')
        def to_th_reduce():
            while not self.stop_flag.is_set():
                try:
                    self._map_result_watcher()
                except Exception as e:
                    print(e)
                    raise
            self.logger.info(f'worker {self.id} th_map_result_watch loop finish')
        th_map_result_watch = threading.Thread(target = to_th_reduce, args = [],
                                               name = f'{self.id} worker watcher')
        self.th_map_result_watch = th_map_result_watch
        th_map = threading.Thread(target = to_th_map, args = [], name=f'{self.id} worker map')
        self.th_map = th_map
         # if set False, must implement mechanism to call _reduce
        # such as calling reduce at the end of dispatch
        
        # to subclass below
        
        
        # TO-DO initialize remote connection
    def _is_deleting(self):
        return self in self.worker_manager.removinged_worker_list and len(self) > 0
    def _is_deleted(self):
        return self in self.worker_manager.removinged_worker_list and len(self) == 0
    
    def graceful_stop(self):
        self.stop_flag.set()
        self.th_map.join()
        self.graceful_stopped.set()
    def get_next_task(self):
        return self.queue_pending._first()
    def __len__(self): # number of total task
        return len(self.queue_pending) + len(self.queue_dispatched)
    def __lt__(self, other):
        return len(self) < len(other)
        
    def __getitem__(self):
        pass
    
    def start(self):
        self.last_run = time.time() - self.min_time_lapse_ns / 1e9
        self.th_map.start()
        self.th_map_result_watch.start()
        
    @abstractmethod
    def dispatch(self, *args, **kwargs):
        # by default, args will be task_info only in form of ((task_time, task, uuid), task_data )
        pass
    @abstractmethod
    def prepare_for_dispatch(self):
        pass
    
    @abstractmethod
    def _dispatch(self, *args, **kwargs):
        # by default, args will be task_info only in form of ((task_time, task, uuid), task_data )
        # do something
        self.dispatch()
        # do something too
        pass
    
    def map(self, *arg, **kwarg):
        """
        map: holds queue pending, wait for queue_dispatched
        reduce: holds queue_dispatched
        """
        self.map_called += 1
        self.logger.debug(f"Worker{self.id}.map called {self.map_called}")
        self.logger.debug(f"Worker{self.id}.map acquiring queue_pending.mutex")
        with self.queue_pending.mutex:
            self.logger.debug(f"Worker{self.id}.map acquired queue_pending.mutex")
            while (len(self.queue_pending) <= 0) and not self.stop_flag.is_set():
                try:
                    self.logger.debug(f"Worker{self.id}.map waiting queue_pending.not_empty")
                    self.queue_pending.not_empty.wait(timeout = 2)
                except:
                    continue
            if self.stop_flag.is_set():
                return False
            self.logger.debug(f"Worker{self.id}.map waited queue_pending.not_empty")
            task_info = self.queue_pending._popleft()
            if len(self.queue_pending) > 0:
                self.queue_pending.not_empty.notify()
                self.queue_pending.not_full.notify()
        if task_info is not None:
            data_for_dispatch = self.prepare_for_dispatch(task_info)
            self.logger.debug(f"Worker{self.id}.map acquring queue_dispatched.mutex")
            with self.queue_dispatched.mutex:
                self.logger.debug(f"Worker{self.id}.map acqured queue_dispatched.mutex")
                while (len(self.queue_dispatched) >= self.dispatched_task_limit) and not self.stop_flag.is_set():
                    self.logger.debug(f"Worker{self.id}.map waiting queue_dispatched.not_full")
                    try:
                        self.queue_dispatched.not_full.wait(timeout = 2)
                    except:
                        continue
                if self.stop_flag.is_set():
                    return False
                self.logger.debug(f"Worker{self.id}.map waited queue_dispatched.not_full")
                
                self._dispatch(data_for_dispatch)
                self.logger.debug(f"Worker{self.id}.map returned from dispatch")
                self.queue_dispatched._put(task_info[0], task_info[1]) # (time, id), user data
                self.logger.debug(f"Worker{self.id}.map dispatched_task_limit notifying")
                if len(self.queue_dispatched) < self.dispatched_task_limit:
                    self.queue_dispatched.not_full.notify()
                else:
                    self.logger.debug(f"Worker{self.id}.map dispatched_task_limit full reached")
                self.queue_dispatched.not_empty.notify()
                self.logger.debug(f"Worker{self.id}.map dispatched_task_limit notified")
        else:
            self.logger.warning("empty still passed not_empty condition")
            
        self.logger.debug(f"Worker{self.id}.map queue_pending notify")


    @abstractmethod
    def reduce (self,dispatch_result= None, task_info = None, map_result = None):
        """
        override this method to implement reduce behavour, default send back to task result queue
        """
        print(f"last step: {task_info} \nresults received with result type={type(map_result)}")
    @abstractmethod
    def _parse_task_info(self):
        return True
    
    def _map_result_watcher(self, task_info = None, **kwarg):
        "watch_event_for_reduce, get map result and call _reduce at the end"
        while not self.stop_flag.is_set():
            self.logger.debug('start listen map result')
            from queue import Empty
            try:
                result = self.map_result_buffer.get(timeout=1)
            except Empty:
                continue
            self.logger.debug('map result read')
            success, task_info, map_result = self._parse_task_info(result)
            if success:
                self._reduce(map_result = map_result, task_info = task_info)
        return map_result
    
    def _reduce(self, map_result, task_info,  *args, **kwargs  ): 
        self.reduce_called+=1
        self.logger.debug(f"Worker{self.id} reduce called {self.reduce_called}")
        self.logger.debug(f"Worker{self.id}._reduce acquiring queue_dispatched.mutex")
        with self.queue_dispatched.mutex:
            self.logger.debug(f"Worker{self.id}._reduce acquired queue_dispatched.mutex")
            if task_info in self.queue_dispatched.queue:
                task_id = task_info[1]
                self.logger.debug("_reduce waiting q_task_completed.not_full")
                
                self.logger.debug(f"Worker{self.id}._reduce finish")
                # MAY NOT NEED NOTIFY
                self.queue_dispatched.queue.pop(task_info)
                self.logger.debug(f"Worker{self.id}._reduce queue_dispatched notify")
                self.queue_dispatched.not_full.notify()
                if len(self.queue_dispatched.queue) > 0:
                    self.queue_dispatched.not_empty.notify()
                self.logger.debug(f"Worker{self.id}._reduce update sort")
                self.worker_manager.update_sort(worker=self, index_in_sorter=self.index_in_sorter, change = -1)
                self.logger.debug(f"Worker{self.id}._reduce update sorted")
                self.reduce(task_info ,dispatch_result = map_result)
                self.logger.debug(f"Worker{self.id}._reduce reduced")
            else:
                self.logger.info(f"late arrival, drop Task info={task_info}")
                
                "most likely task is running remotely"
                
                "task not exist, maybe reduced by other worker, or this is a retry but the original task"
                "completed when retry task is pending, i.e. original may race with current, when retry function implemented"
                pass
        with self.task_manager.q_task_completed.mutex:
            self.logger.debug(f"Worker{self.id}._reduce acquired q_task_completed mutex")
            
            while (not self.stop_flag.is_set() 
                   and self.task_manager.q_task_completed._qsize() >= self.task_manager.q_task_completed_limit):
                self.logger.debug(f"Worker{self.id}._reduce q_task_completed is full")
                try:
                    self.task_manager.q_task_completed.not_full.wait(timeout = 2)
                except:
                    continue
            if self.stop_flag.is_set() :
                return False # will be treated as unfinished dispatch
            self.logger.debug(f"Worker{self.id}._reduce acquired q_task_completed.not_full")
            record = (task_info, map_result)
            self.task_manager.q_task_completed._put(record)
            self.task_manager.q_task_completed.not_empty.notify()
    def add_task(self, id, data, *arg, priority = None):
        # no nesting
        # by default, first in should be prioritised such that redoing old task
        # will have highest priority
        # bug here when stoped it will return wrong !!!!!!!!!!!
        # in lock: q_task_outstanding
        if priority is None:
            priority = time.time()
        self.logger.debug(f"Worker{self.id}.add_task acquiring queue_pending.mutex")
        with self.queue_pending.mutex:
            self.logger.debug(f"Worker{self.id}.add_task acquired queue_pending.mutex")
            while (len(self.queue_pending) >= self.pending_task_limit) and not self.stop_flag.is_set():
                self.logger.debug(f"Worker{self.id}.add_task acquired queue_pending.mutex")
                try:
                    self.queue_pending.not_full.wait(timeout = 2)
                except:
                    continue
            if self.stop_flag.is_set():
                return False
            self.queue_pending._put((priority,id), data)
            self.logger.debug(f"Worker{self.id}.add_task before notify not empty")
            self.queue_pending.not_empty.notify()
            task_assignment[id] = {
                            'task_data': data, 
                            "assigned_to" : self.id} # worker id
            self.worker_manager.update_sort(worker = self, index_in_sorter=0, change = 1)
        self.logger.debug(f"Worker{self.id}.add_task finish")
        return True
    def add_task2 (self, *arg, priority = None):
        if priority is None:
            priority = time.time()
        self.logger.debug(f"Worker{self.id}.add_task2 acquiring queue_pending.mutex")
        with self.queue_pending.mutex:
            self.logger.debug(f"Worker{self.id}.add_task2 has acquried queue_pending.mutex")
            while (not self.stop_flag.is_set() and 
                   (self.queue_pending._qsize() >= self.queue_pending.maxsize)):
                self.logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_full")
                try:
                    self.queue_pending.not_full.wait(timeout = 2)
                except:
                    continue
            if self.stop_flag.is_set():
                return
            self.logger.debug(f"Worker{self.id}.add_task2 queue_pending is not full")
            
            self.logger.debug(f"Worker{self.id}.add_task2 acquiring q_task_outstanding.not_empty")
            with self.task_manager.q_task_outstanding.not_empty:
                self.logger.debug(f"Worker{self.id}.add_task2 has acquired q_task_outstanding.not_empty")
                while (len(self.task_manager.q_task_outstanding) <= 0) and not self.stop_flag.is_set():
                    try:
                        self.logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_empty")
                        self.task_manager.q_task_outstanding.not_empty.wait(timeout = 2)
                    except:
                        continue
                if self.stop_flag.is_set():
                    return False
                self.logger.debug(f"Worker{self.id}.add_task2 is waiting for queue_pending.not_empty")
                packed = self.task_manager.q_task_outstanding._first()
                if packed is None:
                    self.logger.warning("empty but still has not_empty, work number tracker has error")
                    
                else:
                    task_id, data = packed
                    self.queue_pending._put((priority,task_id), data)
                    self.task_manager.q_task_outstanding._popleft()
                    
                    self.queue_pending.not_empty.notify()
                    task_assignment[id] = {
                            'task_data': data, 
                            "assigned_to" : self.id} # worker id
                    self.worker_manager.update_sort(worker=self, index_in_sorter=0, change = 1)
                    
                    if len(self.task_manager.q_task_outstanding) > 0:
                        self.task_manager.q_task_outstanding.not_empty.notify()
                    if self.queue_pending._qsize() > self.queue_pending.maxsize:
                        pass
                    else:
                        self.task_manager.q_task_outstanding.not_full.notify()
                        
                    self.logger.debug(f"Worker{self.id}.add_task2 leaving")

class Local_Thread_Producer_Sider_Worker(Worker):
    def __init__(self, *arg, is_local = True, **kwarg):
        self.is_local = True
        super().__init__( *arg, is_local = self.is_local, **kwarg)
        
        # self.client_socket = None not needed
        self.stop_flag = threading.Event()
        self.last_run = None
        self.packet_cnt = 0
        self.n_max_task = 24
        self.task_queue = queue.Queue()
        
        self.uuid_to_time : Dict[uuid.UUID, float] = {}
        self.th_worker : threading.Thread = threading.Thread(target = self._workload, name = 'thread-worker')
        self.min_time_lapse_ns = 6000
    def start(self):
        super().start()
        self.th_worker.start()
    def reduce(self, task_info ,dispatch_result): 

        # override this function if you want to do something before reducing to the main queue after collect from worker
        # dispatch_result : same as map_result you pass to self._reduce
        return None
    def dispatch( *args, **kwargs):
        pass
    def prepare_for_dispatch(self, task_info,  *args, **kwargs):
        # this function allows developer to do something to prepare data
        # before sending to worker
        return task_info

    def _dispatch(self, *args, **kwargs):
        # this has to be implemented to tell what exactly to do to dispatch data to worker
        # such as what protocol e.g. protobuf, to use to communicate with worker and also #
        # transfer data or ref to data
        self.logger.debug(f"Worker{self.id}.dispatch started dispatch")
        
        
        self.uuid_to_time[args[0][0][1]] = args[0][0][0]
        self.logger.debug(f"Worker{self.id}.dispatch serialised")

        
        cur_time = time.time()
        
        time_diff = self.min_time_lapse_ns / 1e9 - cur_time + self.last_run
        if time_diff > 0:
            self.logger.debug(f"Worker{self.id}.dispatch sleep for {time_diff}")
            time.sleep(time_diff)
        self.packet_cnt+=1
        while not self.stop_flag.is_set():
            
            if self.task_queue.mutex.acquire(timeout= 2):
                break
            else:
                continue
        if self.stop_flag.is_set():
            return
        
        if len(self.task_queue.queue) >= self.task_queue.maxsize:
            while not self.stop_flag:
                if self.task_queue.not_full.wait(timeout =2):
                    break
        self.task_queue._put(args[0])
        self.task_queue.not_empty.notify()
        self.task_queue.mutex.release()
        
        self.last_run = cur_time
    def _workload(self):
        # worker.handle_receive
        while not self.stop_flag.is_set():
            while not self.stop_flag.is_set():
                if self.task_queue.mutex.acquire(timeout=2):
                    break
            if self.stop_flag.is_set():
                continue
            if len(self.task_queue.queue) == 0:
                while not self.stop_flag.is_set():
                    if self.task_queue.not_empty.wait(timeout= 2):
                        break
            if self.stop_flag.is_set():
                continue
            logging.debug(f'local worker{self.id} task q now not empty')
            task_info, task_data = self.task_queue._get()
            self.task_queue.not_full.notify()
            self.task_queue.mutex.release()
            self.uuid_to_time[task_info[1]] = task_info[0]
            # worker.workload
            result = self.workload(task_info,task_data)
            # dispatcher.result_buffer_collect
            use_self_buffer = True
            
            
            if use_self_buffer:
                
                while not self.stop_flag.is_set():
                    if self.map_result_buffer.mutex.acquire(timeout = 2):
                        break
                if self.stop_flag.is_set():
                    continue
                if len(self.map_result_buffer.queue) >= self.n_max_task:
                    while not self.stop_flag.is_set():
                        if self.map_result_buffer.not_full.wait(timeout= 2):
                            break
                logging.debug(f'local worker{self.id} task q now not empty')
                self.map_result_buffer._put((task_info, result))
                self.map_result_buffer.not_empty.notify()
                self.map_result_buffer.mutex.release()
            else:
                result_queue = self.task_manager.q_task_completed
                while not self.stop_flag.is_set():
                    if result_queue.mutex.acquire(timeout = 2):
                        break
                if self.stop_flag.is_set():
                    continue
                if len(result_queue.queue) >= self.task_manager.q_task_completed_limit:
                    while not self.stop_flag.is_set():
                        if result_queue.not_full.wait(timeout= 2):
                            break
                logging.debug(f'local worker{self.id} task q now not empty')
                result_queue._put((task_info, result))
                result_queue.not_empty.notify()
                result_queue.mutex.release()
        self.logger.info(f'local thread worker {threading.current_thread().ident} leaving loop')
    def workload(self, *args, **kwargs):
        #to be overridden
        pass
    def _map_result_watcher(self):
        super()._map_result_watcher()
        self.logger.info(f'local thread result watch {threading.current_thread().ident} leaving loop')
    def _parse_task_info(self,task_info, result, *arg, **kwarg):
        uuid = task_info[1]
        task_time = self.uuid_to_time.get(uuid)
        if task_time is not None:
            task_info = (self.uuid_to_time[uuid], uuid)
            success = True
            map_result = result
        else: # previous retry already solved in a race condition
            success = False
            task_info = None
            map_result = None
        
        return success, task_info, map_result
from pytaskqml.task_worker import parse_data, DataIngressIncomplete, DataCorruptionError
from types import MethodType
class Socket_Producer_Side_Worker(Worker):
    def init_socket(self):
        pass
    def __init__(self,  remote_info, *arg, is_local = False,  **kwarg):
        self.is_local = False
        super().__init__(*arg, is_local = self.is_local, **kwarg)
        self.uuid_to_time = {}
        self.min_time_lapse_ns = 10000
        self.packet_cnt = 0
        self.last_run = time.time() - self.min_time_lapse_ns/ 1e9
        
        self.auto_call_reduce = False
        self.stop_flag = threading.Event()
        self.socket_last_run = None
        self.server_address = remote_info # eg example : ('localhost', 5000)
        self.client_socket = ReconnectingSocket(self.server_address)
        
        self.th_result_buffer_collect : threading.Thread
        self.th_map_result_watch: threading.Thread
        self.checksum_on = False
        self.socket_start_time = time.time()
        
        self.watermark = time.time()
        self.output_minibatch_size = 24
        self._send = MethodType(base_socket_worker._send, self)
    def start(self):
        self.client_socket.connect()
        super().start()
        
        
        th = threading.Thread(target = self.result_buffer_collect, args = [], name = f'result buffer collect {self.id}')
        self.th_result_buffer_collect = th
        self.th_result_buffer_collect.start()
    def graceful_stop(self):
        # how to graceful stop if user has created something that need special care to take down
        # this example turns off reconnecting to let the socket thread able to shut down on disconnect
        # instead of forever trying to reconnect and can never graceful stop
        super().graceful_stop()
        
        self.th_map_result_watch.join()
        if self.is_local:
            pass
        else:
            self.client_socket.retry_enabled = False
        
    def result_buffer_collect(self):
        # this function is require to implement what to do when receive data from variable
        # self.client_socket
        checksum_on = self.checksum_on
        client_socket = self.client_socket
        client_socket.sock.settimeout(2)
        data_with_checksum = bytes()
        self.socket_last_run = time.time()
        cnt = 0
        while not self.stop_flag.is_set():
            try:
                try:
                    data = client_socket.recv(115757000)
                except socket.timeout as te:
                    continue # check for stop event
                if data is None:
                    continue
                data_with_checksum = data_with_checksum + data
                self.logger.debug('worker data received')
                while not self.stop_flag.is_set() and (len(data_with_checksum) >= self.min_start_processing_length):
                    try:
                        length, received_data, calculated_checksum = parse_data(data_with_checksum, checksum_on=checksum_on)
                    except DataIngressIncomplete as e:
                        break
                    except DataCorruptionError:
                        self.logger.debug("from worker receive cond Checksum mismatch. Data corrupted.")
                        client_socket.close()
                        return
                    data_with_checksum = data_with_checksum[checksum_on*32+10+length:]
                    try:
                        self.map_result_buffer.put(received_data, timeout = 1)
                    except queue.Full: # drop message
                        continue
                    time_now =  time.time() 
                    cnt += 1
                    self.logger.debug(f'worker_data integrity confirmed packet no: {cnt}, checksum correct calculated_checksum={calculated_checksum}')
                    self.socket_last_run = time_now

            except ConnectionResetError as e:
                # TO-DO graceful stop
                client_socket.close()
                return
            except UnicodeDecodeError:
                client_socket.close()
                return
            except OSError:
                client_socket.close()
                return
            except Exception as e:
                import traceback
                traceback.print_exc()
                client_socket.close()
                return
            
    def reduce(self, task_info ,dispatch_result): 

        # override this function if you want to do something before reducing to the main queue after collect from worker
        # dispatch_result : same as map_result you pass to self._reduce
        return None
    def _map_result_watcher(self):
        super()._map_result_watcher()
        self.logger.info(f'socket thread result watch {threading.current_thread().ident} leaving loop')
    def _parse_task_info(self, single_buffer_result):
        "implement your own way such that: task_info = (time.time, messageuuid)"
        # response = lipsync_schema_pb2.ResponseData()
        # response.ParseFromString(result) 
        # result = {'messageuuid': response.messageuuid, 'face': np.array(response.face).reshape(96,96,3)}
        # if response.messageuuid not in self.uuid_to_time:
        #     # previous run result stuck on socket buffer
        #     print(f"previous run result stuck on socket buffer response.messageuuid={response.messageuuid}")
        #     success = False
        # task_info = (self.uuid_to_time[response.messageuuid], response.messageuuid)
        success = True
        task_info = None
        map_result = None
        return success, task_info, map_result
        
    def prepare_for_dispatch(self,task_info,  *args, **kwargs):
        # this function allows developer to do something to prepare data
        # before sending to worker
        return task_info
    def dispatch(self, *args):
        """_summary_

        Args:
            message (_type_): _description_

        Returns:
            _str_: _Serialized dispatched data_
            
        implement own logic to create outmessage from args    
        """
        outmessage = ""
        return outmessage
    def _dispatch(self, *args, **kwargs):
        # this has to be implemented to tell what exactly to do to dispatch data to worker
        # such as what protocol e.g. protobuf, to use to communicate with worker and also #
        # transfer data or ref to data
        self.logger.debug(f"Worker{self.id}.dispatch started dispatch")
        
        serialized_data = self.dispatch(*args)
        self.uuid_to_time[args[0][0][1]] = args[0][0][0]
        self.logger.debug(f"Worker{self.id}.dispatch serialised")
        # Calculate checksum
        length = len(serialized_data) 
        if length > 1e10:
            raise ValueError("send: data frame size cannot be greater thatn 1e10 bytes")
        
        self._send(client_socket = self.client_socket, 
                   serialized_data = serialized_data, 
                   checksum_on = self.checksum_on)
        

def config_aware_worker_factory(is_local, remote_info = None, 
                                local_factory: object = Local_Thread_Producer_Sider_Worker,
                                remote_factory: object = Socket_Producer_Side_Worker, *arg, **kwarg):
    if not (bool(is_local) ^ bool(remote_info)):
        raise ValueError('is_local is True then cnanot specify remote_info')
    if is_local:
        return local_factory(*arg, is_local = is_local, **kwarg)
    else:
        return remote_factory( *arg, is_local = is_local, remote_info = remote_info, **kwarg)
              
class Worker_Sorter():
    """
    worker are created here,
    tasks get from elsewhere
    """
    worker_list : List[Worker] = []
    def __init__(self, sorting_algo, task_manager:"Task_Worker_Manager", worker_factory=Worker ):
        self.sorting_algo = sorting_algo
        self.worker_factory = worker_factory
        
        # These two list can potentially form an abstraction of worker group
        self.worker_list :List[Worker] = [] 
        self.removinged_worker_list :List[Worker] = [] # removing and removed worker
        
        self.worker_by_id : Dict[int,Worker] = {}
        self.new_worker_id = 0
        self.graceful_stopped = threading.Event()
        self.worker_list_lock = threading.Lock()
        self.removinged_worker_list_lock = threading.Lock()
        # alias
        self.worker_dispatched : Dict[int, dictQueue] = {}
        self.worker_pending : Dict[int, dictQueue] = {}
        self.task_manager = task_manager
        self.logger = self.task_manager.logger
        self.task_retry_timeout = self.task_manager.task_retry_timeout
        self.retry_watcher_on = self.task_manager.retry_watcher_on
        self.q_task_completed = self.task_manager.q_task_completed
        self.stop_flag = threading.Event()
        for config in task_manager.worker_config:
            self.add_worker(config)

        
    def update_sort(self, worker:Worker, index_in_sorter: int, change : int):
        if change > 0:
            _siftup(worker.worker_queue, index_in_sorter)
        else:
            _siftdown(worker.worker_queue, index_in_sorter, len(self.worker_list)-1)
    def get_worker(self, id = None, index_in_sorter = None):
        # get freest worker if both id and index_in_sorter is None
        return self.worker_list[0]
    def add_worker(self, config: worker_config):
        if config is None:
            config = {'location':'local', "min_start_processing_length":42}
        
        with self.worker_list_lock:
            new_worker = self.worker_factory(
                worker_manager = self,
                id = self.new_worker_id, 
                is_local = config['location'] == 'local', 
                remote_info = None if config['location'] == 'local' else config['location'],
                index_in_sorter = len(self.worker_list),
                worker_queue = self.worker_list,
                min_start_processing_length = config.get("min_start_processing_length")
            )
            heappush(self.worker_list, new_worker )
            #self.worker_list.append(new_worker)
            self.worker_by_id[self.new_worker_id] = new_worker
            self.new_worker_id += 1
    def pop_worker(self, worker: Worker):
        if type(worker) is int: # assume passed worker id instead of worker obj
            worker = self.worker_by_id[worker]
        
        with self.worker_list_lock:
            if worker in self.worker_list:
                # make sure the reference removed is itself
                with self.removinged_worker_list_lock:
                    heappush(self.removinged_worker_list, worker)
                    worker.worker_queue = self.removinged_worker_list
                assert worker is heappop(self.worker_list, worker.index_in_sorter)
            else:
                raise(ValueError("Worker not found in _worker_sorter"))
        return worker
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
        self.logger.debug("Worker_sorter.add_task finding freest worker")
        if worker_id is None:
            free_worker = self.get_freest_worker()
        else:
            free_worker = self.worker_by_id[worker_id]
        self.logger.debug("Worker_sorter.add_task freest worker found")
        success = free_worker.add_task( task_id, data)
        if success:
            pass
        else:
            return False
            
        task_assignment[task_id] = {
                         'task_data': data, 
                         "assigned_to" : free_worker.id} # worker id
        self.update_sort(worker = free_worker, index_in_sorter=0, change = 1)
        return True
    def add_task2(self, *arg, worker_id = None):
        self.logger.debug("Worker_sorter.add_task2 finding freest worker")
        if worker_id is None:
            free_worker = self.get_freest_worker()
        else:
            free_worker = self.worker_by_id[worker_id]
        self.logger.debug("Worker_sorter.add_task2 freest worker found")
        free_worker.add_task2()
            
        
        
    def pop_task(self, task_id, worker_id = None):
        if worker_id is None:
            worker_id = task_assignment[task_id]['assigned_to']
    

            
    def check_workers_timeout_retry(self):
        while not self.stop_flag.is_set():
            for worker in self.worker_by_id.values(): 
                # such that removing workers will also get checked, and if time out, will dispatch to the non removing outstanding queue
                if self.stop_flag.is_set():
                    continue
                with worker.queue_dispatched.not_empty:
                    if (worker.queue_dispatched._qsize() <= 0):
                        continue
                    
                    while worker.queue_dispatched._qsize() > 0:
                        task_info, task_data = worker.queue_dispatched._first()
                        timenow = time.time()
                        if timenow - task_info[0] > self.task_manager.task_retry_timeout:
                            worker.queue_dispatched.queue.pop(task_info)
                            self.update_sort(worker = worker, index_in_sorter=worker.index_in_sorter, change = -1)
                            """
# issue:                         assign holds: queue_pending ,  waiting for : q_task_outstanding
                                    map holds: queue_pending        waiting for : queue_dispatched
            check_workers_timeout_retry holds: queue_dispatched     waiting for : q_task_outstanding
            ding ding: dead lock cycle
                            """                        
                            if self.task_manager.dispatch(task_data, ignore_size_check = True): # use outstanding
                                worker.queue_dispatched.not_full.notify()
                            if worker.queue_dispatched._qsize() > 0:
                                worker.queue_dispatched.not_empty.notify()
                        else:
                            worker.queue_dispatched.not_empty.notify()
                            break # to next worker
                        
            
            time.sleep(max(self.task_retry_timeout, 0.2))
        self.logger.info('retry watch loop escaped')
        
    def start(self):
        with self.worker_list_lock:
            for id, w in self.worker_by_id.items():
                w.start()
                self.logger.info(f"worker {w.id} started")
        if self.task_retry_timeout == float('inf') or self.task_retry_timeout == 0:
            pass
        else:
            th_retry_watch = threading.Thread(target= self.check_workers_timeout_retry, name = 'worker timeout watcher')
            if self.retry_watcher_on:
                th_retry_watch.start()
            self.th_retry_watch = th_retry_watch
        self.logger.info(f"{self} started")
    def graceful_stop(self):
        self.stop_flag.set()
        
        for id, worker in self.worker_by_id.items():
            threading.Thread(target = worker.graceful_stop).start()
            self.logger.info(f'worker {id} started stopping')
        for id, worker in self.worker_by_id.items():
            worker.graceful_stopped.wait()
            self.logger.info(f'worker {id} gracefully stopped')
        if self.retry_watcher_on:
            self.th_retry_watch.join()
            self.logger.info(f'retry watch stopped')
        self.graceful_stopped.set()
def q_cond_wait(self: "Task_Worker_Manager", queue: queue.Queue, max_size: int):
    while not self.stop_flag.is_set() and (queue._qsize() < max_size):
                        
        self.logger.debug("Task_manager.on_task_complete wait more record for minibatch")
        try:
            self.q_task_completed.not_empty.wait(timeout = 2)
        except Exception as e:
            self.logger.debug(f"Task_manager.on_task_complete empty_waited_count={self.empty_waited_count}")
            continue
        self.empty_waited_count += 1
        self.logger.debug(f"Task_manager.on_task_complete q size={self.q_task_completed._qsize()}")

class Task_Worker_Manager():
    """
    Entry point of object
    tasks are created here,
    worker get from elsewhere
    """
    def on_shutdown(self):
        
        pass
    def __init__(self, worker_sorter_algo = 'heapq',
                 worker_sorter_factory = Worker_Sorter,
                 worker_factory = config_aware_worker_factory,
                 worker_config = None,
                 output_minibatch_size = 1,
                 task_retry_timeout = 3,
                 task_stale_timeout = 6,
                 management_port = 59842, 
                 logger = None,
                 min_start_processing_length = None,
                 retry_watcher_on = False,
                 ):
        if logger is None:
            logger = logging.getLogger('__file__')
        self.logger: logging.Logger = logger
        if worker_config is not None:
            self.n_worker = len(worker_config)
        self.assign_task_called = 0 
        self.max_outstanding = 100
        
        self.task_retry_timeout = task_retry_timeout  #float("inf")
        self.task_stale_timeout = task_stale_timeout # float("inf")
        self.worker_sorter_factory = worker_sorter_factory
        self.retry_watcher_on = retry_watcher_on
        if min_start_processing_length is not None:
            self.min_start_processing_length = int(min_start_processing_length)
        else:
            self.min_start_processing_length = min_start_processing_length
        if worker_config is None:
            worker_config = []
        self.worker_config = worker_config
        self.q_task_outstanding: OrderedDictQueue = ODictQueue(maxsize=50)  # pop first often, prioritize unoften, error re-enter from workign on
        self.q_task_completed = queue.PriorityQueue() # frames order is important to allow smooth streaming
        self.worker_pending : Dict[int, dictQueue] # = {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_dispatched : Dict[int, dictQueue] #= {id: dictQueue(maxsize=50) for id in range(self.n_worker)}
        self.worker_sorter_algo=worker_sorter_algo # [heapq, bincount]
        self.q_task_completed_limit = 1000
        self.th_assign = None
        self.th_clean_up = None
        self.stop = False
        self.stop_flag = threading.Event()
        self.watermark = time.time()
        self.water_mark_lookback = 60
        self.watermark_check = False
        self._worker_sorter = self.worker_sorter_factory(sorting_algo = 'heapq', task_manager = self, worker_factory=worker_factory)
        self.worker_pending = self._worker_sorter.worker_pending
        self.worker_dispatched = self._worker_sorter.worker_dispatched
        self.worker_factory = self._worker_sorter.worker_factory
        self.init_locks()
        self.management_port = management_port
        self.graceful_stopped = threading.Event()
        self.empty_waited_count : int = 0 # stat counter for output wait
        self.output_minibatch_size = output_minibatch_size
        self.complete_count = 0
    def graceful_stop(self):
        self.stop_flag.set()
        self._worker_sorter.graceful_stop()
        self._worker_sorter.graceful_stopped.wait()
        self.th_assign.join()
        self.th_clean_up.join()
        self.graceful_stopped.set()
        
    def set_manage_httpd(self):
        mytaskworker = self
        class MyHandler(BaseHTTPRequestHandler):
            def _send_response(self, status_code, message):
                self.send_response(status_code)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(message.encode('utf-8'))

            def do_GET(self):
                if self.path == '/shutdown':
                    mytaskworker.on_shutdown()
                    mytaskworker.stop_flag.set()
                    mytaskworker.graceful_stop()
                    self._send_response(200, "Shutdown requested\n")
                    time.sleep(1)
                    mytaskworker.logger.info('shutting down gracefully')
                    threading.Thread(target=httpd.shutdown).start()
                else:
                    self._send_response(404, "Not found\n")

        
        server_address = ('', self.management_port)
        httpd = HTTPServer(server_address, MyHandler)
        self.httpd = httpd
        def start_server():
            
            httpd.serve_forever()
        self.th_httpd = threading.Thread(target = start_server)
        self.th_httpd.start()

        
        
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
        self.set_manage_httpd()
        th = threading.Thread(target = self.assign_task, args = [], name='task work manager assign task')
        th.start()
        self.th_assign = th
        self._worker_sorter.start()
        
        th = threading.Thread(target = self.on_task_complete, args = [], name='task work manager task complete')
        th.start()
        self.th_clean_up = th
        
    def send_output(self,fresh_output_minibatch):
        # to be overwrrtten to achieve actual task to output the data such as disk or network
        print(f"len fresh output = {len(fresh_output_minibatch)}")

    def assign_task(self):
        """
        assign task from outstanding to worker
        """
        self.assign_task_called += 1
        self.logger.debug(f"Task_Worker_Manager.assign_task called {self.assign_task_called}")
        while not self.stop_flag.is_set():
            try:
                with self.q_task_outstanding.mutex:
                    while (len(self.q_task_outstanding) <= 0) and not self.stop_flag.is_set():
                        try:
                            self.logger.debug("q_task_outstanding empty, wait not empty")
                            self.q_task_outstanding.not_empty.wait(timeout = 2)
                        except Exception as e:
                            continue
                    if self.stop_flag.is_set():
                        continue
                    packed = self.q_task_outstanding._popleft()
                    if len(self.q_task_outstanding) > 0:
                        self.q_task_outstanding.not_empty.notify()
                task_id, data = packed
                success = self._worker_sorter.add_task(task_id, data)
                if not success:
                    continue
                    
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
        self.logger.debug("Task_manager.dispatch start")
        if kwarg.get('task_id') is None:
            task_id = str(uuid.uuid1())
        else: 
            task_id = kwarg.get('task_id')
        self.logger.debug("Task_manager.dispatch acquiring q_task_outstanding.mutex")
        try:
            self.q_task_outstanding.mutex.acquire(timeout=2)
        except:
            return False
        self.logger.debug("Task_manager.dispatch has acquired q_task_outstanding.mutex")
        while (not self.stop_flag.is_set() and 
               ((len(self.q_task_outstanding) >= self.max_outstanding) 
                and not ignore_size_check)):
            try:
                self.logger.debug("q_task_outstanding full, wait not f")
                self.q_task_outstanding.not_full.wait(timeout=2)
            except Exception as e:
                continue
        self.logger.debug("q_task_outstanding not full")
        if self.stop_flag.is_set():
            self.q_task_outstanding.mutex.release()
            return False
            
        while task_id in self.q_task_outstanding.queue:
            task_id = str(uuid.uuid1())
        self.q_task_outstanding._put(task_id, data)
        
        self.q_task_outstanding.not_empty.notify()
        self.q_task_outstanding.mutex.release()
    def print_all_queues(self):
        print_all_queues(self)
    def check_task_stale(self, completed_task):
        task_time = completed_task[0]
        if time.time() - task_time[0] > self.task_stale_timeout:
            self.logger.debug(f"task {completed_task[0]} stale")
            return True
        else:
            self.logger.debug(f"task {completed_task[0]} complete")
        return False
    def output_minibatch(self, mini_batch_to_process):
        output_minibatch = [()] * len(mini_batch_to_process)
        i_cnt_fresh = 0
        for completed in mini_batch_to_process:
            if self.check_task_stale(completed) == False:
                output_minibatch[i_cnt_fresh] = completed
                i_cnt_fresh += 1
        output_minibatch = output_minibatch[:i_cnt_fresh]
        cnt_fresh = len(output_minibatch)
        self.complete_count += cnt_fresh
        self.logger.debug(f'fresh cnt {cnt_fresh}')
        self.logger.info(f'batch done {self.complete_count}')
        self.send_output(output_minibatch[:cnt_fresh])
        self.logger.debug(f'fresh cnt {cnt_fresh} sent')
        return output_minibatch
    def on_task_complete(self):
        #function that operate on task completed
        self.logger.debug("Task_manageron.on_task_complete start")
        while not self.stop_flag.is_set():
            
            self.logger.debug("Task_manager.on_task_complete start waiting for minibatch")
            with self.q_task_completed.not_empty:
                q_cond_wait(self, self.q_task_completed, self.output_minibatch_size)
                if self.stop_flag.is_set():
                    continue
                if self.q_task_completed._qsize() >= self.output_minibatch_size:
                    self.logger.debug("Task_manager.on_task_complete waited minibatch")
                    mini_batch_to_process = [()] * self.output_minibatch_size
                    for i in range(self.output_minibatch_size):
                        mini_batch_to_process[i] = self.q_task_completed._get()
                    self.q_task_completed.not_full.notify()
                    if self.q_task_completed._qsize() > 0:
                        self.q_task_completed.not_empty.notify()
            output_minibatch = self.output_minibatch(mini_batch_to_process)
                
from pytaskqml.task_worker import myclient, base_socket_worker, Stop_Signal
from queue import Queue, Empty
class Socket_Input_Task_Worker_Manager(Task_Worker_Manager):
    def __init__(self, *args, data_ingress_type = 'socket',
                 data_ingress_info = "localhost:41230", **kwargs):
        super().__init__(*args, **kwargs)
        self.data_ingress_type = data_ingress_type
        self.data_ingress_info = data_ingress_info
        server_address = data_ingress_info.split(':')
        self.server_address = (str(server_address[0]), int(server_address[1]))
        self.client_dict: Dict[int, myclient] = {}
        self.th_ingress: threading.Thread = None

        self.ingress_lock_on = False
         # since one is get the other put, the put side need no lock and can speed up
        self.set_up_ingress_data()
        self.handle_receive = MethodType(base_socket_worker.handle_receive, self)
    @abstractmethod
    def deserialise_parsed_input(self):
        pass
    @abstractmethod
    def handle_receive(self):
        pass
    def set_up_ingress_data(self):
        if self.data_ingress_type == 'socket':
            self.th_ingress = threading.Thread(target = self.serve_socket_server, args = [])
    
    def graceful_stop(self):
        
        for client_id, client in self.client_dict.items():
            client.stop_event.set()
            
        if self.th_ingress is not None:
            self.th_ingress.join()
        return super().graceful_stop()
    def handle_workload(self, client:myclient):
        cnt_dispatched = 0
        while not client.stop_event.is_set():
            self.logger.debug(f"client {client.id} handle_workload waiting get_parsed_data")
            try:
                parsed_data = client.received_parsed_queue.get(timeout = 1)
            except Empty:
                continue
            # if client.received_parsed_queue.mutex.acquire(timeout = 1):
            #     if len(client.received_parsed_queue.queue) <= 0:
            #         while not client.stop_event.is_set():
            #             if client.received_parsed_queue.not_empty.wait(timeout=1):
            #                 break
            #         if client.stop_event.is_set():
            #             client.received_parsed_queue.mutex.release()
            #             continue
            #     self.logger.debug(f"client.received_parsed_queue not empty len = {len(client.received_parsed_queue.queue)}")
            #     parsed_data = client.received_parsed_queue.get_nowait()
            #     client.received_parsed_queue.mutex.release()
            # else:
            #     continue
            if parsed_data is Stop_Signal: 
                break
            self.logger.debug(f"client {client.id} handle_workload get_parsed_data pre-dispatch")
            self.dispatch(**self.deserialise_parsed_input(parsed_data))
            cnt_dispatched += 1
        self.logger.info("Socket Dispatcher leaving handle_workload")
    def handle_socket(self,client: myclient):
        self.logger.debug(f"client {client.id} handle_socket socket receive waiting")
        client.th_recv = threading.Thread(target = self.handle_receive, args = [client], name = "handle_receive")
        client.th_recv.start()
        # loop process reopen socket
        client.th_process = threading.Thread(target = self.handle_workload, args = [client], name = "handle_workload")
        client.th_process.start()
        # loop send
        #client.th_send = threading.Thread(target = self.handle_send, args = [client], name = "handle_send")
        #client.th_send.start()

        client.th_recv.join()
        client.th_process.join()
        #client.th_send.join()
        self.logger.debug(f"client {client.id} handle_socket client {client.id} exits")
    
    def serve_socket_server(self):
        while not self.stop_flag.is_set():
            
            try:
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                self.server_socket.bind(self.server_address)
                self.logger.info("start socket server started")
                
            except:
                self.server_socket.close()
                continue
            cnt_client_id = 0
            self.server_socket.settimeout(1)
            while not self.stop_flag.is_set():
                try:
                    self.server_socket.listen(1)

                    # Accept a client connection
                    client_socket, client_address = self.server_socket.accept()
                    
                    self.logger.info(f"start accepted client {client_address}")
                    client_socket.settimeout(2)
                    client = myclient(client_socket, id=cnt_client_id)
                    self.client_dict[cnt_client_id] = client
                    cnt_client_id += 1
                    th = threading.Thread(target = self.handle_socket, args = [client])
                    th.start()
                    self.logger.info(f'dispatcher added client {cnt_client_id}')
                    print(f'dispatcher added client {cnt_client_id}')
                except socket.timeout as te:
                    # its ok
                    pass
                except Exception as e:
                    self.logger.debug(traceback.format_exc())
                    pass

    def start(self):
        
        self.th_ingress.start()
        
        return super().start()
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
