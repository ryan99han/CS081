from multiprocessing.pool import ThreadPool
import threading
import time
import random

MAX_THREADS = 10
print_lock = threading.Lock()

def thread_function(num):
    with print_lock:
        print("Processed %i", num)

if __name__ == "__main__":
    pool = ThreadPool(processes=MAX_THREADS)
    results = []
    lst = [i for i in range(25)]
    while lst:
        num = lst.pop()
        results.append(pool.apply_async(thread_function, (num, )))

    pool.close() # Stop adding tasks to pool
    pool.join()  # Wait for all tasks to complete
