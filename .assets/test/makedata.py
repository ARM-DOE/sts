#!/usr/bin/env python3

import os
import sys
import time
import datetime
import subprocess


def makedata(name):
    now = datetime.datetime.fromtimestamp(
        time.time()
    ).strftime('%Y%m%d.%H%M%S')
    root = '%s/run/data/out/%s' % (
        os.path.dirname(os.path.realpath(__file__)), name
    )
    root = os.path.abspath(root)
    tags = {
        'xl': {'size': 100*1024*1024,  'count': 3},
        'lg': {'size': 15*1024*1024,   'count': 7},
        'md': {'size': 5*1024*1024,    'count': 2},
        'sm': {'size': 50*1024,        'count': 1000},
        'xs': {'size': 500,            'count': 100},
    }
    for t in sorted(tags.keys()):
        td = tags[t]
        tdir = '%s/%s' % (root, t)
        if not os.path.exists(tdir):
            os.makedirs(tdir)
        for i in range(td['count']):
            path = "%s/%s.%s.%06d" % (tdir, t, now, i)
            with open(os.devnull, 'w') as devnull:
                subprocess.call([
                    "dd",
                    "if=/dev/zero",
                    "of=%s.lck" % path,
                    "bs=%d" % td['size'], "count=1"
                ], stdout=devnull, stderr=devnull)
            os.rename("%s.lck" % path, path)


def main(argv):
    while True:
        makedata('stsin-1')
        makedata('stsin-2')
        time.sleep(60)


if __name__ == '__main__':
    main(sys.argv[1:])
