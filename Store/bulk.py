#!/usr/bin/env python

from google.appengine.ext import db
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.api import datastore_errors
from google.appengine.ext.mapreduce import operation as op
from google.appengine.ext.mapreduce import context

import logging

import store

def yield_put(entity):
    f = op.db.Put(entity)
    f(context.get())
    #yield f

def touch(key):
    # change entity
    app = key.namespace()
    kind = key.kind()
    id = key.id_or_name()
    
    ctx = context.get()
    params = ctx.mapreduce_spec.mapper.params
    matching_app = params['app_to_process']

    if matching_app and matching_app != app:
        return

    metadata_entity = store._GetMetadataEntity(app)    
    store.update_entity(app, kind, id, {}, metadata_entity, put_function=yield_put, rebuild_facets=True)
