#!/usr/bin/env python

from google.appengine.ext import db
from google.appengine.ext.webapp import util
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import datastore_errors
from google.appengine.api import lib_config
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.api import oauth
import simplejson
import logging
import re
import urllib
import datetime
import simplejson as json
import iso8601

class _ConfigDefaults(object):
  def auth_token():
      return None
  def superuser_auth_token():
      return None

_config = lib_config.register('manta_', _ConfigDefaults.__dict__)

APP_METADATA = "AppMetadata"
SPECIAL_KINDS = [ APP_METADATA ]

FACET_PREFIX = '_facet_'

_builtin_facets = { 
  'app': ['officerid'],
  'app2': ['officerid'],
  'sample': ['officerid'],
  'juhudi': ['officerid'],
  'juhuditest': ['officerid'],
}
def GetFacetsForApp(app, metadata_entity):
  if metadata_entity:
    if 'facets' in metadata_entity:
      facets = metadata_entity['facets']
      if facets and isinstance(facets, list):
        return facets
    return []
  if app in _builtin_facets:
    return _builtin_facets[app]
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
    
  if id is None and app in SPECIAL_KINDS:
    id = kind
    kind = app
    app = ''
  return (app, kind, id)

def memcache_key(app, kind, id):
  return "entity:" + (app or '') + '/' + (kind or '') + '/' + (id or '')

READ = "READ"
WRITE = "WRITE"
OWNER = "OWNER"
AUTH_LEVELS = [ None, READ, WRITE, OWNER ]

def GetCurrentUser():
  user = None
  try:
    user = oauth.get_current_user()
  except:
    user = users.get_current_user()
  return user

def AuthTokenValid(entity, key, user_value):
  if not key in entity:
    return False
  stored_value = entity[key]
  if stored_value == None:
    return False
  if stored_value == '*':
    return True
  if stored_value == user_value:
    return True
  return False

def UserValid(entity, key, user_value):
  if user_value is None:
    return False
  if not key in entity:
    return False
  email = user_value.email()
  stored_value = entity[key]
  if stored_value == None:
    return False
  if not isinstance(stored_value, list):
    return False
  if '*' in stored_value:
    return True
  if email in stored_value:
    return True
  return False

def IncreaseAuth(auth_level, value):
  if not value in AUTH_LEVELS:
    return auth_level
  if AUTH_LEVELS.index(value) > AUTH_LEVELS.index(auth_level):
    return value
  return auth_level

def GetMetadataEntity(request):
  # Get AppMetadata
  # Try memcache
  # If not present, get entity
  #   Add to memcache 1m expiry if found
  (app, kind, id) = extract_path(request.path)
  if app is '':
    # The id specifies the app we are actually interested in
    app = id

  key = memcache_key('', 'AppMetadata', app)
  data = memcache.get(key)
  if data is not None:
    return data
  else:
    entity = get_entity('', 'AppMetadata', app)
    entity_dict = {}
    entity_dict.update(entity)
    memcache.add(key, entity_dict, 60)
    return entity_dict

def GetAuthLevel(request, metadata_entity):
  # If metadata not present, check default auth from appengine_config
  #   return OWNER or None
  #
  # Get auth-token header
  # Determine highest level of auth provided from auth-token
  #
  # Get current user
  # Determine highest level of auth provided from user
  # 
  # Return auth level

  auth_token = request.headers.get('Auth-Token',"")
  
  # If present, the superuser_auth_token() allows override of all
  # priveleges.
  if _config.superuser_auth_token():
    if auth_token == _config.superuser_auth_token():
      return OWNER

  # Otherwise, the appengine_config.auth_token() provides only a
  # default level of access.
  if not metadata_entity:
    if _config.auth_token():
        if auth_token == _config.auth_token():
          return OWNER
        else:
          return None
    else:
        return OWNER

  auth_level = None

  if AuthTokenValid(metadata_entity, 'read_auth_token', auth_token):
    auth_level = IncreaseAuth(auth_level, READ)
  if AuthTokenValid(metadata_entity, 'write_auth_token', auth_token):
    auth_level = IncreaseAuth(auth_level, WRITE)
  if AuthTokenValid(metadata_entity, 'owner_auth_token', auth_token):
    auth_level = IncreaseAuth(auth_level, OWNER)

  user = GetCurrentUser()
    
  if UserValid(metadata_entity, 'read_users', user):
    auth_level = IncreaseAuth(auth_level, READ)
  if UserValid(metadata_entity, 'write_users', user):
    auth_level = IncreaseAuth(auth_level, WRITE)
  if UserValid(metadata_entity, 'owner_users', user):
    auth_level = IncreaseAuth(auth_level, OWNER)

  return auth_level

def IsAuthorized(app, kind, id, request_auth_level, minimum_auth_level):
  if app is '':
    # Disallow any kinds not specifically allowed
    if not kind in SPECIAL_KINDS:
      return False
    # Disallow any directory requests for Metadata
    if id is None:
      return False
    return request_auth_level == OWNER

  if not request_auth_level in AUTH_LEVELS:
    return False
  if not minimum_auth_level in AUTH_LEVELS:
    return False
  return AUTH_LEVELS.index(request_auth_level) >= AUTH_LEVELS.index(minimum_auth_level)

def IsEncryptionSufficient(request, metadata_entity):
  if not metadata_entity:
    return True
  if request.scheme == 'https':
    return True
  if 'https_required' in metadata_entity:
    return not metadata_entity['https_required']
  return True

def update_entity(app, kind, id, data, metadata_entity, put_function=None, rebuild_facets=False):
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
    facets = GetFacetsForApp(app, metadata_entity)

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
    memcache.delete(memcache_key(app, kind, id))
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

def get_entities(app, kind, metadata_entity, params=None):
    query = datastore.Query(kind=kind, namespace=app)
    facets = GetFacetsForApp(app, metadata_entity)
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

