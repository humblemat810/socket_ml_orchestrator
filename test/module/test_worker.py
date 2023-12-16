import pytest
from pytaskqml.task_dispatcher import Task_Worker_Manager, Worker_Sorter, Local_Thread_Producer_Sider_Worker, config_aware_worker_factory, Worker
from pytaskqml.task_worker import base_socket_worker, base_worker
def empty_task_manager():
    my_task_worker_manager = Task_Worker_Manager(
               worker_factory=config_aware_worker_factory, 
               worker_config = [],
               output_minibatch_size = 24,
               management_port = 8000
           )
    return my_task_worker_manager

@pytest.fixture()
def manager_single_thread_worker():
    mt_task_manager = empty_task_manager()
    mt_task_manager._worker_sorter.add_worker({'location': "local", "min_start_processing_length":1})
    return mt_task_manager


def test_basic_import():
    from pytaskqml.task_worker import Stop_Signal, base_worker, base_socket_worker

def test_dispatcher_side_worker_workload(manager_single_thread_worker: Task_Worker_Manager):
    my_local_thread_worker: Local_Thread_Producer_Sider_Worker = manager_single_thread_worker._worker_sorter.worker_by_id[0]
    my_local_thread_worker._workload()
    assert True

def worker_side_worker_workload():

    assert True