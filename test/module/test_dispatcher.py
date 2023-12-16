import pytest
from pytaskqml.task_dispatcher import Task_Worker_Manager, Worker_Sorter, Socket_Producer_Side_Worker, config_aware_worker_factory, Worker

def empty_task_manager():
    my_task_worker_manager = Task_Worker_Manager(
               worker_factory=config_aware_worker_factory, 
               worker_config = [],
               output_minibatch_size = 24,
               management_port = 8000
           )
    return my_task_worker_manager

def test_empty_manager():
    mt_task_manager = empty_task_manager()
    assert mt_task_manager._worker_sorter.worker_list == []
    mt_task_manager._worker_sorter.add_worker({'location':'local', "min_start_processing_length":42})
    assert len(mt_task_manager._worker_sorter.worker_list) == 1
    assert issubclass(type(mt_task_manager._worker_sorter.worker_list[0]), Worker)
    mt_task_manager._worker_sorter.add_worker({'location':("localhost",12345), "min_start_processing_length":42})
    assert len(mt_task_manager._worker_sorter.worker_list) == 2
    
@pytest.fixture()
def manager_single_socket_worker():
    mt_task_manager = empty_task_manager()
    mt_task_manager._worker_sorter.add_worker({'location':("localhost",12345), "min_start_processing_length":1})
    return mt_task_manager
import threading
import time
def test_socket_worker_collect(manager_single_socket_worker: Task_Worker_Manager):
    
    import socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost",12345))
    server_socket.listen(1)
    def send_sample_response():
        client_socket, client_address = server_socket.accept()
        client_socket.send('0000000005hello'.encode())
    # th_ssr = threading.Thread(target = send_sample_response)
    # th_ssr.start()
    threading.Thread(target = send_sample_response).start()
    server_side_socket_worker: Socket_Producer_Side_Worker= manager_single_socket_worker._worker_sorter.worker_by_id[0]
    server_side_socket_worker.client_socket.connect()
    time.sleep(0.5)
    def result_buffer_collect():
        server_side_socket_worker.result_buffer_collect()
    th = threading.Thread(target = result_buffer_collect, name = 'test_result_buffer_collect')
    th.start()
    
    time.sleep(0.5)
    server_side_socket_worker.stop_flag.set()
    server_side_socket_worker.client_socket.retry_enabled = False
    server_socket.close()
    assert(len(server_side_socket_worker.map_result_buffer.queue) == 1)
    assert server_side_socket_worker.map_result_buffer.queue[0].decode() == 'hello'
    
from types import MethodType
import pytaskqml.task_dispatcher as task_dispatcher
import inspect
from typing import List
members = inspect.getmembers(task_dispatcher)
from abc import ABCMeta

@pytest.fixture()
def manager_with_each_worker():
    mt_task_manager = empty_task_manager()
    mt_task_manager._worker_sorter.add_worker({'location':("localhost",12345), "min_start_processing_length":1})
    mt_task_manager._worker_sorter.add_worker({'location':"local", "min_start_processing_length":1})
    return mt_task_manager
# classes = [i for i in members if inspect.isclass(i[1])]
# dispatcher_side_worker_subclasses : List[Worker] = [i[1] for i in classes if issubclass(i[1], Worker) and i[1] is not Worker]
# @pytest.mark.parametrize("myworker_class", dispatcher_side_worker_subclasses)
def test_parse_task_info_reduce(manager_with_each_worker: Task_Worker_Manager):
    for id, myworker in manager_with_each_worker._worker_sorter.worker_by_id.items():
        cnt = 0
        from collections import namedtuple
        def monkey_patch_counter(self, result):
            nonlocal cnt
            assert result == b'helloworld'
            Parsed_Task_Info = namedtuple("Parsed_Task_Info", ['success', 'task_info', 'map_result'])
            cnt += 1
            return Parsed_Task_Info(success=True, task_info= 'hello', map_result=  b'helloworld')
            
        myworker._parse_task_info = MethodType(monkey_patch_counter, myworker) 
        myworker.map_result_buffer.put(b'helloworld')
        
        cnt2 = 0
        def monkey_patch_counter2(self, map_result, task_info):
            nonlocal cnt2
            assert map_result == b'helloworld'
            assert task_info == 'hello'
            cnt2 += 1
        myworker._reduce = MethodType(monkey_patch_counter2, myworker)
        myworker.th_map_result_watch.start()
        time.sleep(1)
        myworker.stop_flag.set()
        
        assert cnt == 1
        assert cnt2 == 1
    
