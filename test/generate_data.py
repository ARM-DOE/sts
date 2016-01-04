import os
import sys
import time
import random
import tempfile

# --- USAGE ---
# python generate_data.py /sender/watch/directory

alph = list("abcdefghijklmnopqrstuvwxyz")

MIN_SIZE = 0
MAX_SIZE = 1e7
WRITE_BLOCK_SIZE = 10000
MIN_PATH_ADDITIONS = 0
MAX_PATH_ADDITIONS = 4


def randBytes(num):
    count = 0
    while count < num:
        count += 1
        yield random.choice(alph)


def getBlock(num):
    return "".join(list(randBytes(num)))


def generatePath(base_path):
    path_parts = [base_path]
    path_adds = random.randint(MIN_PATH_ADDITIONS, MAX_PATH_ADDITIONS) + 1
    for x in xrange(path_adds):
        path_parts.append(("".join(list(randBytes(5)))))
    return os.path.sep.join(path_parts) + ".test"


def generateFile(path):
    try:
        os.makedirs(os.path.dirname(path))
    except Exception:
        pass
    fi = open(path, "w")
    file_size = random.randint(MIN_SIZE, MAX_SIZE)
    sys.stdout.write("Generating file of size " + str(file_size) + " ... ")
    sys.stdout.flush()
    passes_needed = file_size / WRITE_BLOCK_SIZE
    for block_no in xrange(passes_needed):
        fi.write(getBlock(WRITE_BLOCK_SIZE))
    fi.close()
    sys.stdout.write("done\n")
    sys.stdout.flush()


if __name__ == "__main__":
    while 1:
        temp_dir = tempfile.gettempdir()
        new_path = generatePath(temp_dir)
        new_file_name = os.path.split(new_path)[-1]
        generateFile(new_path)
        os.utime(new_path, (time.time(), time.time()))
        os.rename(new_path, os.path.join(sys.argv[1], new_file_name))
