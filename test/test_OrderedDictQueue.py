def test_OrderedDictQueue():
    from pytaskqml.utils.OrderedDictQueue import IndexedPriorityQueue

    myipq = IndexedPriorityQueue()

    myipq.queue.insert('a', 0)
    myipq.queue.insert('b', 2)
    myipq.queue.insert('c', 1)
    for i in range((len(myipq.queue.heap)-1)//2):
        assert myipq.queue.heap[i][1] <= myipq.queue.heap[2*i+1][1] 
        assert myipq.queue.heap[i][1] <= myipq.queue.heap[2*i+2][1] 
    assert (myipq.queue.pop_by_index('c') == ('c', 1))
    myipq.queue.insert('d', 1)
    myipq.queue.update('d', -1)
    last = myipq.queue.get_min()
    assert last == ('d', -1)
    while not myipq.queue.is_empty():
        cur = myipq.queue.pop_by_index(myipq.queue.get_min()[0])
        assert last[1] <= cur[1]
    try:
        myipq.queue.get_min()
    except Exception as e:
        assert e.args[0] == "Priority queue is empty."
    