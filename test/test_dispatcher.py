import pytest
from pytaskqml.task_dispatcher import Task_Worker_Manager, Worker_Sorter, config_aware_worker_factory
@pytest.fixture
def fixt_task_manager(request):
    print(dir(request))
    my_task_worker_manager = Task_Worker_Manager(
                worker_factory=config_aware_worker_factory, 
                worker_config = request.param,
                output_minibatch_size = 24,
                management_port = 8000
            )
    return my_task_worker_manager
@pytest.fixture
@pytest.mark.parametrize("fixt_task_manager", [[]], indirect=True)
def empty_task_manager(fixt_task_manager):
    return fixt_task_manager
def test_empty_manager_creation(empty_task_manager: Task_Worker_Manager):
    assert empty_task_manager._worker_sorter.worker_list == []