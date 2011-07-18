#!/usr/bin/env python
#
# Take input file
# While lines remaining:
#   Read N lines
#   Upload lines
#
# Alt: upload, store in blobstore, defer processing using task queue

import logging
import os
import signal
import sys
import urllib2
import time
import datetime
import platform
import threading
import socket

CHUNK_SIZE = 30
NUM_THREADS = 15
NUM_RETRIES = 10
AUTH_TOKEN = ""
TIMEOUT = 15
MAX_IDLE_SECONDS = 60

class WorkBundle:
    def __init__(self, lines, header):
        self.lines = lines
        self.header = header
        self.total_lines = 0
        for b in lines:
            self.total_lines += len(b)
        self.uploaded_lines = 0
        self._lock = threading.Lock()
        self.done = False
        self.error = False
        self.last_status = ""

    def Start(self):
        self.start = time.time()
        self.last = self.start
        self.last_progress = self.start
        self.last_lines = 0

    def Stop(self):
        self._lock.acquire()
        self.done = True
        self._lock.release()

    def is_done(self):
        return self.done

    def is_error(self):
        return self.error

    def ReportError(self):
        self._lock.acquire()
        try:
            self._ReportErrorUnlocked()
        finally:
            self._lock.release()

    def _ReportErrorUnlocked(self):
        self.error = True
        self.done = True

    def GetWork(self):
        self._lock.acquire()
        try:
            if self.done:
                return None
            if len(self.lines) > 0:
                return self.lines.pop()
            return None
        finally:
            self._lock.release()

    def UpdateStats(self, uploaded_lines, status):
        self._lock.acquire()
        try:
            self.uploaded_lines += uploaded_lines
            self.last_status = status
        finally:
            self._lock.release()

    def DisplayProgress(self):
        self._lock.acquire()
        try:
            now = time.time()
            elapsed = datetime.timedelta(seconds=(now - self.start))
            delta_lines = self.uploaded_lines - self.last_lines
            if delta_lines > 0:
                self.last_progress = now
            expected_completion = ""
            if self.uploaded_lines > 0:
                expected_completion = datetime.timedelta(seconds=((now - self.start) / self.uploaded_lines) * (self.total_lines - self.uploaded_lines))
            logging.info("Uploaded %d of %d. Left: %s Elapsed: %s Rate: %1.1f l/sec Received: %s" % (
                self.uploaded_lines, self.total_lines, expected_completion, 
                elapsed, ((self.uploaded_lines - self.last_lines) / float(now - self.last)), self.last_status))
            self.last = now
            self.last_lines = self.uploaded_lines
            last_progress_elapsed = now - self.last_progress
            if (last_progress_elapsed) > MAX_IDLE_SECONDS:
                logging.error("Elapsed %s seconds without progress. Aborting upload.", last_progress_elapsed)
                self._ReportErrorUnlocked()
        finally:
            self._lock.release()
        

class WorkerThread(threading.Thread):
    def __init__(self, url, bundle):
        self.url = url
        self.bundle = bundle
        self.should_exit = False
        threading.Thread.__init__(self)
        
    def run(self):
        while not self.should_exit:
            l = self.bundle.GetWork()
            if l is None:
                break
            req = urllib2.Request(url=self.url, data=self.bundle.header + "".join(l))
            req.add_header('Content-type', 'text/csv')
            req.add_header('Auth-Token', AUTH_TOKEN)
            success = False
            for i in xrange(NUM_RETRIES):
                try:
                    f = urllib2.urlopen(req, timeout=TIMEOUT)
                    success = True
                    break
                except (urllib2.URLError, urllib2.HTTPError), e:
                    logging.error("Attempt %d/%d. Error opening request: %s", i+1, NUM_RETRIES, e)
                if self.bundle.is_done():
                    break
                # Sleep between attempts.
                time.sleep(2)
            if success:
                self.bundle.UpdateStats(len(l), f.read().strip())
            else:
                self.bundle.ReportError()
                break

class StatusThread(threading.Thread):
    def __init__(self, bundle):
        self.bundle = bundle
        threading.Thread.__init__(self)

    def run(self):
        while not self.bundle.is_done():
            time.sleep(5)
            self.bundle.DisplayProgress()
        if self.bundle.is_error():
            # This is particularly ugly. This is required because on
            # Windows, socket() holds an exclusive lock on
            # getaddrinfo(), which causes all network connections to
            # hang if there is no network connection. That means all
            # the threads just sit there, with 15 second timeouts,
            # which isn't fun for anybody.
            #
            # One solution would be to stop using urllib2, switch to
            # raw httplib, and add our own lock around open(), with a
            # busy-loop, so we don't get stuck piled on that lock. But
            # that's tons of work. Instead, we'll just kill the
            # process.
            if platform.system() == 'Windows':
                logging.error("Forcibly exiting process with SIGTERM.")
                os.kill(os.getpid(), signal.SIGTERM)


def RefreshSettings():
    try:
        import upload_agent
        config = upload_agent.config
    except ImportError:
        config = None
    if config:
        globals()['NUM_THREADS'] = config['upload']['num_threads']
        globals()['AUTH_TOKEN'] = config['upload']['auth_token']

def upload_file(url, filename):
    RefreshSettings()

    socket.setdefaulttimeout(TIMEOUT)

    f = open(filename)
    header = f.readline()
    lines = f.readlines()
    line_bundles = []
    while len(lines) > 0:
        line_bundles += [ lines[0:CHUNK_SIZE] ]
        del lines[0:CHUNK_SIZE]

    if len(line_bundles) == 0:
        logging.warning("No lines to upload for file %s. Upload complete.", 
                        os.path.basename(filename))
        return True

    bundle = WorkBundle(line_bundles, header)

    status_thread = StatusThread(bundle)
    threads = []
    bundle.Start()
    for i in xrange(NUM_THREADS):
        thread = WorkerThread(url, bundle)
        thread.start()
        threads += [thread]

    forced_exit = False
    status_thread.start()
    try:
        while True:
            threads_alive = False
            for i in threads:
                i.join(1)
                threads_alive = threads_alive or i.isAlive()
            if not threads_alive:
                break
    except KeyboardInterrupt:
        forced_exit = True
        bundle.ReportError()
        logging.error("Killing threads...")
        for i in threads:
            i.should_exit = True

    bundle.Stop()
    status_thread.join()
    if forced_exit:
        raise KeyboardInterrupt()
    return not bundle.is_error()

def main():
    upload_file(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
