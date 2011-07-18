#!/usr/bin/env python

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext.webapp import util
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import datastore_errors
import logging
import re
import urllib
import datetime
import csv
import StringIO
import store

NUMBER_RE = re.compile(
    r'(-?(?:0|[1-9]\d*))?(\.\d+)?([eE][-+]?\d+)?',
    (re.VERBOSE | re.MULTILINE | re.DOTALL))

NULL_VALUE = "__null__"

def CleanUpFormats(row):
    for key in row:
        value = row[key]
        if type(value) is not str:
            continue
        if key.endswith('id'):
            continue
        if key.endswith('date'):
            continue
        if len(value) == 0:
            continue

        m = NUMBER_RE.match(value)
        if m is not None and m.end() == len(value):
            integer, frac, exp = m.groups()
            if exp and not integer:
                continue
            if frac or exp:
                res = float((integer or '') + (frac or '') + (exp or ''))
            else:
                res = int(integer)
            row[key] = res

def ImportSplitFile(app, body_file):
    rows = 0

    segments = body_file.getvalue().split("\n\n")
    for seg in segments:
        buf = StringIO.StringIO(seg)
        header = buf.readline().lstrip("#")
        kind = header.strip()
        reader = csv.DictReader(buf)
        # Hack to support Python 2.5 / 2.6
        if reader.fieldnames is None:
            reader.fieldnames = reader.reader.next()
        for row in reader:
            key = row[reader.fieldnames[0]]
            CleanUpFormats(row)
            datastore.RunInTransaction(
                store.update_entity, app, kind, key, row)
            rows = rows + 1

    return rows

def ImportCSV(app, kind, key_column, body_file):
    error = False
    reader = csv.DictReader(body_file)

    # Hack to support Python 2.5 / 2.6
    if reader.fieldnames is None:
        reader.fieldnames = reader.reader.next()
    for f in reader.fieldnames:
        if ' ' in f or f[0:1].isdigit() or f[0] == '-':
            logging.error('Invalid field name: ' + f)
            error = True

    if (key_column and key_column not in reader.fieldnames):
        error = True
    if not key_column:
        key_column = reader.fieldnames[0]
        
    if error:
        return -1

    rows = 0

    for row in reader:
        key = row[key_column]
        if "kind" in row:
            del row["kind"]
        if "app" in row:
            del row["app"]
        if "key" in row:
            del row["key"]
        
        for r in row:
            if row[r] == NULL_VALUE:
                row[r] = None
            assert r is not None, \
                "Could not split CSV row properly: row field contains an extra comma: " + str(row)
                

        all_null = True
        for r in row:
            if r == key_column:
                continue
            elif row[r] != None:
                all_null = False
                break
        
        if all_null:
            row[key_column] = None

        CleanUpFormats(row)
        datastore.RunInTransaction(
            store.update_entity, app, kind, key, row)
        rows = rows + 1

    return rows

class MainHandler(webapp.RequestHandler):
    def post(self):
        #logging.info("body is " + self.request.body_file.getvalue())
        key_column = self.request.get('key')
        app_column = self.request.get('app')
        kind_column = self.request.get('kind')
        split_file = self.request.get('split_file')

        result = None
        if split_file:
            result = ImportSplitFile(app_column, self.request.body_file)
        else:
            result = ImportCSV(app_column, kind_column, key_column, self.request.body_file)
        
        if result == -1:
            self.response.set_status(404)
            self.response.clear()
            return

        self.response.out.write(result)
        self.response.out.write("\n")

    def get(self):
	self.response.out.write("I am the walrus")

def main():
    application = webapp.WSGIApplication([('/.*', MainHandler)],
                                         debug=True)
    util.run_wsgi_app(application)


if __name__ == '__main__':
    main()
