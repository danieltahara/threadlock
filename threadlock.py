import concurrent.futures
import multiprocessing.pool
import sys


class MyGlobal:
    def __init__(self, use_concurrent=True):
        if use_concurrent:
            self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        else:
            self.pool = multiprocessing.pool.ThreadPool(16)
        self.map = self.pool.map

    def run(self, input_list):
        def echo(v):
            return v

        print(self)
        for res in self.map(echo, input_list * 16 * 2):
            pass
        return None


g = MyGlobal()


def run(input_list):
    g.run(input_list)


def run_ft_pool(input_list, size=8):
    with concurrent.futures.ProcessPoolExecutor(max_workers=size) as pool:
        for _ in pool.map(run, [input_list] * size):
            pass


def run_mp_pool(input_list, size=8):
    pool = multiprocessing.Pool(size)

    for _ in pool.map(run, [input_list] * size):
        pass


def _test_run_pool(fn):
    # g.run([1, 2, 3])
    print(f"Running with {fn.__name__}")
    fn([1, 2])
    # fn([1, 2])
    assert False


def test_run_mp_pool():
    _test_run_pool(run_mp_pool)


def test_run_ft_pool():
    _test_run_pool(run_ft_pool)


if __name__ == "__main__":
    """
    python threadlock.py thread_(futures|mp) (futures|mp) [init_in_main]

    thread_futures X ___ => Okay
    mp X ___ [+ ] => Hangs
    thread_futures X ___ +  init_in_main => Hangs
    """
    multiprocessing.set_start_method("spawn")

    thread_futures = sys.argv[1] == "thread_futures"

    if not thread_futures:
        g = MyGlobal(False)

    futures = sys.argv[2] == "futures"
    fn = test_run_ft_pool
    if not futures:
        fn = test_run_mp_pool

    # Use the executor in the main process BEFORE multiprocesing
    if len(sys.argv) > 3:
        run([1, 2])
    fn()
