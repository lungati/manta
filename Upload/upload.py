#!/usr/bin/env python
#
# Take input file
# While lines remaining:
#   Read N lines
#   Upload lines
#
# Alt: upload, store in blobstore, defer processing using task queue

import sys
import urllib2
import time
import datetime

CHUNK_SIZE = 100
AUTH_TOKEN = ""

def RefreshSettings():
    try:
        import upload_agent
        config = upload_agent.config
    except ImportError:
        config = None
    if config:
        globals()['AUTH_TOKEN'] = config['upload']['auth_token']

def upload_file(url, filename):
    RefreshSettings()

    f = open(filename)
    header = f.readline()
    lines = f.readlines()
    total_lines = len(lines)
    uploaded_lines = 0
    start = time.time()
    last = start
    while len(lines) > 0:
        l = lines[0:CHUNK_SIZE]
        del lines[0:CHUNK_SIZE]
        req = urllib2.Request(url=url, data=header + "".join(l))
        req.add_header('Content-type', 'text/csv')
        req.add_header('Auth-Token', AUTH_TOKEN)
        f = urllib2.urlopen(req)
        uploaded_lines = uploaded_lines + len(l)
        now = time.time()
        elapsed = datetime.timedelta(seconds=(now - start))
        expected_completion = datetime.timedelta(seconds=((now - start) / uploaded_lines) * (total_lines - uploaded_lines))
        print "Uploaded %d of %d. Left: %s Elapsed: %s Rate: %1.1f l/sec Received: %s" % (
            uploaded_lines, total_lines, expected_completion, 
            elapsed, (len(l) / (now - last)), f.read().strip())
        last = now

if __name__ == '__main__':
  upload_file(sys.argv[1], sys.argv[2])
