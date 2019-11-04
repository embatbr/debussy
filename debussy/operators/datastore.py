# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json
import logging

from airflow.models import BaseOperator

from dags.debussy.metadata import DatastoreMetadataReader, DatastoreMetadataWriter, DatastoreMetadataCopy


class DatastoreGetObjectOperator(BaseOperator):

    def __init__(self, project, control, namespace, kind, metadata_converter, task_id=None, filters=[],
        *args, **kwargs):
        
        BaseOperator.__init__(
            self,
            task_id=task_id if task_id else 'datastore_get_{}_{}'.format(namespace, kind),
            *args,
            **kwargs
        )

        self.project = project
        self.control = control
        self.namespace = namespace
        self.kind = kind
        self.filters = filters
        self.metadata_converter = metadata_converter(self)

        self.entity = None

    def execute(self, context):
        metadata_reader = DatastoreMetadataReader(
            self.project, self.control['namespace'], self.control['kind'], self.filters
        )

        res = metadata_reader.fetch()
        self.entity = list(res)[0]
        if self.metadata_converter:
            logging.info(self.metadata_converter(self.entity))
            return self.metadata_converter(self.entity)


class DatastorePutObjectOperator(DatastoreGetObjectOperator):

    def __init__(self, project, control, namespace, kind, metadata_updater, filters=[],
        *args, **kwargs):
        DatastoreGetObjectOperator.__init__(
            self,
            project=project,
            control=control,
            namespace=namespace,
            kind=kind,
            metadata_converter=lambda self: None,
            filters=filters,
            *args,
            **kwargs
        )

        self.task_id='datastore_put_{}_{}'.format(namespace, kind)
        self.metadata_updater = metadata_updater(self)

    def execute(self, context):
        DatastoreGetObjectOperator.execute(self, context)

        if self.metadata_updater:
            self.metadata_updater(context, self.entity)

        metadata_writer = DatastoreMetadataWriter(self.project)
        metadata_writer.update(self.entity)

class DatastoreCopyObjectOperator(DatastoreGetObjectOperator):

    def __init__(self, project, control, source_namespace, source_kind, dest_namespace, dest_kind, metadata_updater, filters=[],
        *args, **kwargs):
        DatastoreGetObjectOperator.__init__(
            self,
            project=project,
            control=control,
            namespace=source_namespace,
            kind=source_kind,
            metadata_converter=lambda self: None,
            filters=filters,
            *args,
            **kwargs
        )
        
        self.task_id='datastore_copy_{}_{}'.format(source_kind, dest_kind)
        self.metadata_updater = metadata_updater(self)
        self.dest_namespace = dest_namespace
        self.dest_kind = dest_kind

    def execute(self, context):
        DatastoreGetObjectOperator.execute(self, context)

        if self.metadata_updater:
            self.metadata_updater(context, self.entity)

        metadata_writer = DatastoreMetadataCopy(self.project, self.dest_namespace, self.dest_kind)
        metadata_writer.update(self.entity)
