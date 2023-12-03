import yappi
def test_ctx_stats():
        from threading import Thread
        DUMMY_WORKER_COUNT = 5
        yappi.start()

        class DummyThread(Thread):
            pass

        def dummy():
            pass

        def dummy_worker():
            pass

        for i in range(DUMMY_WORKER_COUNT):
            t = DummyThread(target=dummy_worker)
            t.start()
            t.join()
        yappi.stop()
        stats = yappi.get_thread_stats()
        return stats
stats = test_ctx_stats()
stats.print_all()
print(stats)