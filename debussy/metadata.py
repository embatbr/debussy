# -*- coding: utf-8 -*-

from google.cloud import datastore


class DatastoreMetadataReader(object):

    def __init__(self, project, namespace, kind, filters=[]):
        self.client = datastore.Client(
            project=project, namespace=namespace
        )

        self.kind = kind
        self.filters = filters

    def fetch(self):
        query = self.client.query(kind=self.kind)
        for filter_obj in self.filters:
            query.add_filter(
                filter_obj['property'], filter_obj['operator'], filter_obj['value']
            )

        return query.fetch()


class DatastoreMetadataWriter(object):

    def __init__(self, project):
        self.client = datastore.Client(project=project)

    def update(self, entity):
        self.client.put(entity)


class DatastoreMetadataCopy(object):
    
    def __init__(self, project, namespace, kind):
        self.client = datastore.Client(project=project, namespace=namespace)
        self.kind = kind

    def update(self, entity):
        entity.key = self.client.key(self.kind)

        self.client.put(entity)
