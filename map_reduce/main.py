from multiprocessing.pool import ThreadPool
from filelock import FileLock
from os import walk

import threading
import time
import random
import string

MAX_THREADS = 1
print_lock = threading.Lock()
EXCLUDE_CHARS = string.punctuation + string.digits

def add_to_word_file(word):
    wordfile = word + ".txt"
    with FileLock(wordfile + ".lock"):
        with open(word_file, 'a') as wf:
            wf.write(word + " 1")

def get_words_from_line(line):
    return line.translate(str.maketrans('', '', EXCLUDE_CHARS)).split(" ")

def mapper(filename):
    filepath = "text/" + filename
    with open(filepath) as fp:
        line = fp.readline()[:-1]
        while line:
            line = get_words_from_line(line)
            for word in line:
                add_to_word_file(word)
            break
            line = fp.readLine()
    fp.close()
    with print_lock:
        print("Processed file: ", filename)

if __name__ == "__main__":
    _, _, filenames = next(walk("text/"))
    filenames = filenames[:1] # DEBUG
    print(filenames)
    pool = ThreadPool(processes=MAX_THREADS)

    while filenames:
        filename = filenames.pop()
        pool.apply_async(mapper, (filename, ))

    pool.close() # Stop adding tasks to pool
    pool.join()  # Wait for all tasks to complete
