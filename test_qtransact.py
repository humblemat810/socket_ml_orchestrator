import queue
import time
from threading import Thread
def test_qtransact():
    q1 = queue.Queue()
    q2 = queue.Queue()
    print(q1.empty())
    def tx1 (q1: queue.Queue,q2: queue.Queue):
        with q1.mutex :
            with q2.mutex:
                time.sleep(2)
                q1._put(1)
                q2._put(2)
    def tx2 (q1: queue.Queue, q2: queue.Queue):
        with q1.mutex:
            with q2.mutex:
                q1._put('a')
                q2._put('b')

    th1 = Thread(target = tx1, args=[q1,q2])
    th1.start()
    time.sleep(1)
    th2 = Thread(target = tx2, args=[q1,q2])
    th2.start()
    th1.join()
    th2.join()
    while not q1.empty():
        print(q1.get_nowait())
    while not q2.empty():
        print(q2.get_nowait())

test_qtransact()
