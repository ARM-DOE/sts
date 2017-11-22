#!/usr/bin/env python3

import os
import sys
import time
import random
import datetime
import subprocess


def makedata(name, prefix, size, count):
    now = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d.%H%M%S')
    root = '%s/data/out/%s' % (os.getenv('STS_HOME', '/var/tmp/sts'), name)
    root = os.path.abspath(root)
    tdir = '%s/%s' % (root, prefix)
    if not os.path.exists(tdir):
        os.makedirs(tdir)
    c = int(count)
    s = int(size)
    count = random.randint(int(c-c*0.5), int(c+c*0.5))
    for i in range(count):
        size = random.randint(int(s-s*0.5), int(s+s*0.5))
        name = "%s.%s.%06d" % (prefix, now, i)
        path = "%s/%s" % (tdir, name)
        with open(os.devnull, 'w') as devnull:
            subprocess.call([
                "dd",
                "if=/dev/urandom",
                "of=%s.lck" % path,
                "bs=%d" % size, "count=1"
            ], stdout=devnull, stderr=devnull)
        os.rename("%s.lck" % path, path)


def main(argv):
    if len(argv) < 5:
        print("Expected 5 args")
        return 1
    (name, prefix, size, count, sleep) = tuple(argv)
    while True:
        makedata(name, prefix, size, count)
        time.sleep(int(sleep))


if __name__ == '__main__':
    main(sys.argv[1:])
