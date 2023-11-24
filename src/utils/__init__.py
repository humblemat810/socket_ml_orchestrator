from task_dispatcher import Task_Worker_Manager
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