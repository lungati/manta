#!/usr/bin/env python

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext.webapp import util
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import datastore_errors
from google.appengine.api import lib_config
import simplejson
import logging
import re
import urllib
import datetime
import simplejson as json
import store
import csv_import

class _ConfigDefaults(object):
  def auth_token():
      return None

_config = lib_config.register('manta_', _ConfigDefaults.__dict__)

def IsAuthTokenValid(request):
    if _config.auth_token():
        return request.headers.get('Auth-Token',"") == _config.auth_token()
    else:
        return True

class MainHandler(webapp.RequestHandler):
    def post(self):
        if not IsAuthTokenValid(self.request):
            self.response.set_status(401)
            self.response.clear()
            return
            
        (app, kind, id) = store.extract_path(self.request.path)
        #logging.info("%s, %s, %s" % (app, kind, id))
        data = self.request.body_file.getvalue()
        data_obj = {}
        if (len(data) == 0):
            return
        
        if (app and kind and id):
            try:
                data_obj = json.loads(data, use_decimal=True)
            except json.JSONDecodeError:
                self.response.set_status(500)
                self.response.clear()
                return

            datastore.RunInTransaction(
                store.update_entity, app, kind, id, data_obj)

        elif self.request.headers['Content-type'].startswith('text/csv') and not id:
            key_column = self.request.get('key', None)
            result = csv_import.ImportCSV(app, kind, key_column, self.request.body_file)

            if result == -1:
                self.response.set_status(500)
                self.response.clear()
                return

            self.response.out.write(result)
            self.response.out.write("\n")

        elif (app and kind and not id):
            try:
                data_obj = json.loads(data, use_decimal=True)
            except json.JSONDecodeError:
                self.response.set_status(500)
                self.response.clear()
                return
            if not isinstance(data_obj, list):
                self.response.set_status(500)
                self.response.clear()
                return
            for data in data_obj:
                if not 'key' in data:
                    self.response.set_status(500)
                    self.response.clear()
                    return

            count = 0
            for data in data_obj:
                datastore.RunInTransaction(
                    store.update_entity, app, kind, data['key'], data)
                count += 1
            
            self.response.out.write(count)
            self.response.out.write("\n")

    def get(self):
        if not IsAuthTokenValid(self.request):
            self.response.set_status(401)
            self.response.clear()
            return

        self.response.headers["Access-Control-Allow-Origin"] = "*"
        self.response.headers["Access-Control-Allow-Headers"] = "Auth-Token"

        (app, kind, id) = store.extract_path(self.request.path)
        #logging.info("%s, %s, %s" % (app, kind, id))
        entity = None
        if (app and kind and id):
            entity = store.get_entity(app, kind, id)
            if entity:
              self.response.out.write(store.output_entity_json(entity))
              self.response.out.write("\n")
              self.response.headers["X-Num-Results"] = str(1)
            else:
              self.response.set_status(404)
              self.response.clear()
              return
        elif (app and kind):
            results = store.get_entities(app, kind, self.request.params)
            self.response.out.write("[\n")
            first = True
            count = 0
            for r in results:
                if not first:
                    self.response.out.write(",\n")
                first = False
                self.response.out.write(store.output_entity_json(r))
                count += 1
            self.response.out.write("\n]\n")
            self.response.headers["X-Num-Results"] = str(count)
        else:
            self.response.set_status(404)
            self.response.clear()
            return

    def options(self):
        self.response.headers["Access-Control-Allow-Origin"] = "*"
        self.response.headers["Access-Control-Allow-Headers"] = "Auth-Token"

def main():
    application = webapp.WSGIApplication([('/.*', MainHandler)],
                                         debug=True)
    util.run_wsgi_app(application)


if __name__ == '__main__':
    main()
