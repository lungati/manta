#!/usr/bin/env python

from google.appengine.ext import db
from google.appengine.ext.webapp import util
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import datastore_errors
import simplejson
import logging
import re
import urllib
import datetime
import simplejson as json
import iso8601

FACET_PREFIX = '_facet_'

# TODO Replace this with a real facets implementation, which uses a
# metadata entity that defines the app, and memcache for speed.
_facets = { 
  'app': ['officerid'],
  'app2': ['officerid'],
}
def GetFacetsForApp(app):
  if app in _facets:
    return _facets[app]
  return []
def is_facet_property(p):
  return p.startswith(FACET_PREFIX)
def facet_property_name(p):
  return FACET_PREFIX + p

class Revision(db.Model):
  type = db.StringProperty(required=True)
  parent_id = db.StringProperty(required=True)
  data = db.TextProperty(required=True, indexed=False)
  date = db.DateTimeProperty(required=True)
  rev = db.StringProperty(required=True)

def extract_path(path):
  parts = re.split('/', path, 4)
  parts = map(urllib.unquote, parts)
  app = parts[1] or None
  kind = None
  if len(parts) > 2:
    kind = parts[2]
  id = None
  if len(parts) > 3:
    id = parts[3]
  return (app, kind, id)

def update_entity(app, kind, id, data, put_function=None, rebuild_facets=False):
    # Start transaction
    # Get entity
    # Apply changes to entity
    # Add new revision (generate revision id & date)
    # Store transaction
    key = datastore.Key.from_path(kind, id, namespace=app)
    newest_date = datetime.datetime.min
    newest_entity = None
    rev = "1"
    date = datetime.datetime.now()
    revision_list = []
    try:
        entity = datastore.Get(key)
        # Entity was fetched. Find all revisions.
        query = datastore.Query(namespace=app)
        query.Ancestor(key)
        for r in query.Run():
            #logging.info(r.kind() + " " + r.key().name())
            if (r.kind() != "Revision"):
                continue
            assert(r.kind() == "Revision")
            if (r['date'] > newest_date):
                newest_entity = r
                newest_date = r['date']
            if rebuild_facets:
              revision_list.append(r)
        if newest_entity:
            rev = str(int(newest_entity['rev']) + 1)
            date = max(date, 
                       newest_entity['date'] + 
                       datetime.timedelta(microseconds=1))
    except datastore_errors.EntityNotFoundError:
       entity = datastore.Entity(kind, name=id, namespace=app)
       assert(entity.key() == key)
    
    # See if an update is even needed.
    existing_data = {}
    for p in entity.keys():
      if p != 'rev' and p != 'date' and not is_facet_property(p):
        existing_data[p] = entity[p]
    for p in data.keys():
      if data[p] is None and not p in existing_data:
        del data[p]
    changed = False
    for p in data.keys():
      if p not in existing_data:
        changed = True
      elif existing_data[p] != data[p]:
        changed = True
      if changed:
        break

    current_facets = {}
    new_facets = {}
    facets = GetFacetsForApp(app)

    for p in entity.keys():
      if is_facet_property(p):
        current_facets[p] = entity[p]

    def find_facets(obj):
      for p in facets:
        if p in obj:
          fp = facet_property_name(p)
          value = obj[p]
          if (not value is None and 
              ((fp in current_facets and
                not value in current_facets[fp]) or
               (not fp in current_facets))):
            if fp not in new_facets:
              new_facets[fp] = []
            if not value in new_facets[fp]:
              new_facets[fp].append(value)

    find_facets(entity)
    find_facets(data)
    for r in revision_list:
      try:
        data = json.loads(r['data'])
        find_facets(data)
      except json.JSONDecodeError:
        logging.error("Could not parse JSON from Revision: " + str(r.key()));

    if not changed and len(new_facets) == 0:
      return

    #logging.info("Facets differ: " + str(new_facets))

    #logging.info("Data differs: " + 
    #             str(set(data.items()).difference(set(existing_data.items()))))
    entity.update(data)
    for p in new_facets:
      if p not in entity:
        entity[p] = new_facets[p]
      else:
        entity[p].extend(new_facets[p])
    for p in entity.keys():
        if entity[p] is None:
          # If we instead do a del entity[p] here, the property would be gone.
          # In this special case, we need to always maintain null properties,
          # so clients can sync this state.
          entity[p] = None
    if changed:
      entity['rev'] = rev
      entity['date'] = date
    if put_function:
      put_function(entity)
    else:
      datastore.Put(entity)
    if changed:
      change = Revision(
        key=datastore.Key.from_path("Revision", rev, 
                                    parent=entity.key(), namespace=app),
        type = kind,
        parent_id = entity.key().id_or_name(),
        data = json.dumps(data, use_decimal=True),
        rev = rev,
        date = date)
      if put_function:
        put_function(change)
      else:
        change.put()

def get_entity(app, kind, id):
    key = datastore.Key.from_path(kind, id, namespace=app)
    entity = None
    try:
        entity = datastore.Get(key)
    except datastore_errors.EntityNotFoundError:
        pass
    return entity

_UNPARSED_SENTINEL = {}

def get_entities(app, kind, params=None):
    query = datastore.Query(kind=kind, namespace=app)
    facets = GetFacetsForApp(app)
    if params:
      for param in params:
        if param == 'date_start':
          query['date >='] = iso8601.parse_date(params.getone('date_start'))
        elif param == 'date_end':
          query['date <'] = iso8601.parse_date(params.getone('date_end'))
        else:
          property_name = param
          if param in facets:
            property_name = facet_property_name(param)
          value = params.getone(param)
          value_native = _UNPARSED_SENTINEL
          try:
            value_native = json.loads(value)
          except json.JSONDecodeError, ValueError:
            pass
          if value_native is _UNPARSED_SENTINEL:
            # If it could not be parsed, assume an unquoted string
            value_native = value
          query[property_name + ' ='] = value_native
    return query.Run()

def output_entity(entity):
    base = {"key": entity.key().id_or_name(),
            "type":entity.kind()}
    output = dict(entity).copy()
    for p in output.keys():
      if is_facet_property(p):
        del output[p]
    output.update(base)
    return output

def output_entity_json(entity):
    obj = output_entity(entity)
    return json.dumps(obj, default=encode_datetime, use_decimal=True)

def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return str(obj)
    raise TypeError(repr(o) + " is not JSON serializable")

