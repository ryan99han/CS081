from multiprocessing.pool import ThreadPool
from filelock import FileLock
from os import walk

import threading
import time
import random
import string

MAX_THREADS = 5
EXCLUDE_CHARS = string.punctuation + string.digits
RESULTS_FILE = "results.txt"
print_lock = threading.Lock()


def add_to_word_file(word):
    word_file = "words/" + word + ".txt"
    with FileLock(word_file + ".lock"):
        with open(word_file, "a") as wf:
            wf.write(f"{word} 1\n")

def get_words_from_line(line):
    return line.translate(str.maketrans('', '', EXCLUDE_CHARS)).split(" ")

def mapper(filename, directory):
    filepath = directory + filename
    with open(filepath) as fp:
        line = fp.readline()
        while line:
            line = get_words_from_line(line[:-1])
            for word in line:
                add_to_word_file(word)
            line = fp.readline()

    with print_lock:
        print("Processed file:", filename)

def reducer(filename, directory):
    filepath = directory + filename
    word, freq = "", 0
    with open(filepath) as fp:
        lines = fp.readlines()
        first_line = lines[0].split(" ")
        word = first_line[0]
        freq = len(lines)

    with FileLock(RESULTS_FILE + ".lock"):
        with open(RESULTS_FILE, "a") as rf:
            rf.write(f"{word}, {freq}\n")

    with print_lock:
        print(f"Processed word: {word}, freq: {freq}")

def get_files_from_directory(directory, exclude=None):
    _, _, filenames = next(walk(directory))
    if exclude is None:
        return filenames

    files = []
    for filename in filenames:
        if not filename.endswith(exclude):
            files.append(filename)

    return files

def run_tasks_on_workers(function, directory, exclude):
    pool = ThreadPool(processes=MAX_THREADS)
    filenames = get_files_from_directory(directory, exclude)

    while filenames:
        filename = filenames.pop()
        pool.apply_async(function, (filename, directory, ))

    pool.close() # Stop adding tasks to pool
    pool.join()  # Wait for all tasks to complete
    with print_lock:
        print("Worker tasks finished")

if __name__ == "__main__":
    run_tasks_on_workers(mapper, "text/", None)
    run_tasks_on_workers(reducer, "words/", ".lock")
    # with open(RESULTS_FILE, "r") as f:
    #     print(len(f.readlines()))
