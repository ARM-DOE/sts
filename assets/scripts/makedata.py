#!/usr/bin/env python3

import os
import sys
import subprocess

def makedata(name):
    root = '%s/run/data/out/%s' % (os.path.dirname(os.path.realpath(__file__)),name)
    root = os.path.abspath(root)
    tags = {
        # 'xl' : { 'size' : 500*1024*1024, 'count' : 3   },
        # 'lg' : { 'size' : 5*1024*1024,   'count' : 25  },
        # 'md' : { 'size' : 500*1024,      'count' : 100 },
        'sm' : { 'size' : 50*1024,       'count' : 10 },
        # 'xs' : { 'size' : 500,           'count' : 50  },
    }
    for t in sorted(tags.keys()):
        td = tags[t]
        tdir = '%s/%s' % (root,t)
        if not os.path.exists(tdir):
            os.makedirs(tdir)
        for i in range(td['count']):
            with open(os.devnull, 'w') as devnull:
                subprocess.call([
                    "dd",
                    "if=/dev/zero",
                    "of=%s/%s.%06d.dat" % (tdir,t,i),
                    "bs=%d" % td['size'],"count=1"
                ], stdout=devnull, stderr=devnull)

def main(argv):
    makedata('stsin-1')

if __name__ == '__main__':
    main(sys.argv[1:])
