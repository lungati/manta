#!/usr/bin/env python

import os
import sys
import time

if 'GOOGLE_APPENGINE_SDK' in os.environ:
    GOOGLE_APPENGINE_SDK = os.environ['GOOGLE_APPENGINE_SDK']
else:
    GOOGLE_APPENGINE_SDK = os.path.join("..", "google_appengine")

def RunCommand(command):
    print command
    os.system(command)

GOOGLE_APPENGINE_SDK = os.path.expanduser(GOOGLE_APPENGINE_SDK)
print "Starting local App Engine Manta Store server..."
dev_appserver_path = os.path.join(GOOGLE_APPENGINE_SDK, "dev_appserver.py")
if not os.path.exists(dev_appserver_path):
    print "ERROR: Google App Engine SDK not found at: " + dev_appserver_path
    print "ERROR: Either provide the SDK at this path, or adjust the GOOGLE_APPENGINE_SDK variable."
    exit()
store_path = os.path.join(os.path.join(os.path.dirname(__file__), ".."), "Store")
datastore_path = os.path.join(os.path.dirname(__file__), "sample.datastore")

RunCommand(('"%s" "%s" --use_sqlite --datastore_path="%s" "%s" & ' % (
            sys.executable, dev_appserver_path, datastore_path, store_path)))

time.sleep(5)
print
print "Done. Server is: localhost:8080"
