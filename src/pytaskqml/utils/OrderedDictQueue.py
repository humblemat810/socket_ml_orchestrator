'''A multi-producer, multi-consumer queue.'''

import threading
from typing import TypeVar

from collections import OrderedDict
from time import monotonic as time


try:
    from _queue import Empty
except ImportError:
    class Empty(Exception):
        'Exception raised by Queue.get(block=0)/get_nowait().'
        pass

class Full(Exception):
    'Exception raised by Queue.put(block=0)/put_nowait().'
    pass


class BaseOrderedDictQueue():
    '''Create a queue object with a given maximum size.

    If maxsize is <= 0, the queue size is infinite.
    '''
    def _first(self):
        """
            lockless version that read, recommend to combine with locks
        """
        for item in self.queue.items():
            return item
    def __len__(self):
        return len(self.queue)
    def _popleft(self):
        
        for k,_ in self.queue.items():
            v = self.queue.pop(k)
            self.not_full.notify()
            return (k, v)
        self.not_full.notify()
        return 
    def __getitem__(self, key):
        return self.queue[key]
    def __setitem__(self, key, value):
        with self.not_empty:
            self.queue[key] = value
            self.not_empty.notify()
    def __init__(self, BaseType, maxsize=0):
        self.maxsize = maxsize
        self._init(BaseType, maxsize)

        # mutex must be held whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = threading.Lock()

        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = threading.Condition(self.mutex)

        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = threading.Condition(self.mutex)

        # Notify all_tasks_done whenever the number of unfinished tasks
        # drops to zero; thread waiting to join() is notified to resume
        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0

    def task_done(self):
        '''Indicate that a formerly enqueued task is complete.

        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items
        placed in the queue.
        '''
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self):
        '''Blocks until all items in the Queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        '''
        with self.all_tasks_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

    def qsize(self):
        '''Return the approximate size of the queue (not reliable!).'''
        with self.mutex:
            return self._qsize()

    def empty(self):
        '''Return True if the queue is empty, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() == 0
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can grow before the result of empty() or
        qsize() can be used.

        To create code that needs to wait for all queued tasks to be
        completed, the preferred technique is to use the join() method.
        '''
        with self.mutex:
            return not self._qsize()

    def full(self):
        '''Return True if the queue is full, False otherwise (not reliable!).

        This method is likely to be removed at some point.  Use qsize() >= n
        as a direct substitute, but be aware that either approach risks a race
        condition where a queue can shrink before the result of full() or
        qsize() can be used.
        '''
        with self.mutex:
            return 0 < self.maxsize <= self._qsize()

    def put(self, key, item, block=True, timeout=None):
        '''Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        '''
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(key,item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def get(self, key = None, block=True, timeout=None):
        '''Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        with self.not_empty:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize():
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get(key = key)
            self.not_full.notify()
            return item

    def put_nowait(self, key, item):
        '''Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        '''
        return self.put(key, item, block=False)

    def get_nowait(self):
        '''Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        '''
        return self.get(block=False)

    # Override these methods to implement other queue organizations
    # (e.g. stack or priority queue).
    # These will only be called with appropriate locks held

    # Initialize the queue representation
    def _init(self, BaseType, maxsize):
        self.queue = BaseType()
        self.maxsize = maxsize

    def _qsize(self):
        return len(self.queue)

    # Put a new item in the queue
    def _put(self, key, item):
        if key in self.queue:
            raise(ValueError(f"Duplicate key found for {key}"))
        self.queue[key] = item

    # Get an item from the queue
    def _get(self, key = None):
        if key is None:
            return self._popleft()
        else:
            return self[key]


class OrderedDictQueue(BaseOrderedDictQueue):
    def __init__(self, maxsize=0):
        super().__init__(BaseType=OrderedDict, maxsize=maxsize)
        pass

class dictQueue(BaseOrderedDictQueue):
    def __init__(self, maxsize=0):
        super().__init__(BaseType=dict, maxsize=maxsize)
        pass
class IndexedPriorityQueue(BaseOrderedDictQueue):
    def __init__(self, maxsize=0):
        self.queue: _IndexedPriorityQueue
        super().__init__(BaseType=_IndexedPriorityQueue, maxsize=maxsize)
        
        pass
class _IndexedPriorityQueue():
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.heap = []
        self.index_map = {}

    def is_empty(self):
        return len(self.heap) == 0

    def size(self):
        return len(self.heap)

    def contains(self, index):
        return index in self.index_map

    def insert(self, index, priority):
        if self.contains(index):
            raise ValueError("Index already exists in the priority queue.")
        self.heap.append((index, priority))
        self.index_map[index] = len(self.heap) - 1
        self._upheap(len(self.heap) - 1)

    def update(self, index, new_priority):
        if not self.contains(index):
            raise ValueError("Index does not exist in the priority queue.")
        heap_index = self.index_map[index]
        old_priority = self.heap[heap_index][1]
        self.heap[heap_index] = (index, new_priority)
        if new_priority < old_priority:
            self._upheap(heap_index)
        else:
            self._downheap(heap_index)

    def pop_by_index(self, index):
        if not self.contains(index):
            raise ValueError("Index does not exist in the priority queue.")
        heap_index = self.index_map[index]
        del self.index_map[index]
        if heap_index == len(self.heap) - 1:
            return self.heap.pop()
        else:
            self._swap(heap_index, len(self.heap) - 1)
            to_return = self.heap.pop()
            self._downheap(heap_index)
            return to_return
    
    def get_min(self):
        if self.is_empty():
            raise ValueError("Priority queue is empty.")
        return self.heap[0]

    def extract_min(self):
        min_element = self.get_min()
        self.delete(min_element[0])
        return min_element

    def _upheap(self, i):
        while i > 0:
            parent = (i - 1) // 2
            if self.heap[i][1] < self.heap[parent][1]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _downheap(self, i):
        while True:
            left_child = 2 * i + 1
            right_child = 2 * i + 2
            smallest = i
            if left_child < len(self.heap) and self.heap[left_child][1] < self.heap[smallest][1]:
                smallest = left_child
            if right_child < len(self.heap) and self.heap[right_child][1] < self.heap[smallest][1]:
                smallest = right_child
            if smallest != i:
                self._swap(i, smallest)
                i = smallest
            else:
                break

    def _swap(self, i, j):
        self.heap[i], self.heap[j] = self.heap[j], self.heap[i]
        self.index_map[self.heap[i][0]] = i
        self.index_map[self.heap[j][0]] = j