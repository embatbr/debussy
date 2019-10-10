# -*- coding: utf-8 -*-

from datetime import datetime as dt
import json

from airflow.models import BaseOperator

from dags.debussy.metadata import DatastoreMetadataReader, DatastoreMetadataWriter


class DatastoreGetObjectOperator(BaseOperator):

    def __init__(self, project, control, namespace, kind, metadata_converter,
        *args, **kwargs):
        BaseOperator.__init__(
            self,
            task_id='datastore_get_{}_{}'.format(namespace, kind),
            *args,
            **kwargs
        )

        self.project = project
        self.control = control
        self.namespace = namespace
        self.kind = kind
        self.metadata_converter = metadata_converter(self)

        self.entity = None

    def execute(self, context):
        metadata_reader = DatastoreMetadataReader(
            self.project, self.control['namespace'], self.control['kind'], [
                {
                    'property': 'namespace',
                    'operator': '=',
                    'value': self.namespace
                },
                {
                    'property': 'kind',
                    'operator': '=',
                    'value': self.kind
                }
            ]
        )

        res = metadata_reader.fetch()
        self.entity = list(res)[0]
        if self.metadata_converter:
            return self.metadata_converter(self.entity)


class DatastorePutObjectOperator(DatastoreGetObjectOperator):

    def __init__(self, project, control, namespace, kind, *args, **kwargs):
        DatastoreGetObjectOperator.__init__(
            self,
            project,
            control,
            namespace,
            kind,
            lambda self: None,
            *args,
            **kwargs
        )

        self.task_id='datastore_put_{}_{}'.format(namespace, kind)

    def execute(self, context):
        DatastoreGetObjectOperator.execute(self, context)

        date_upper_limit = self.xcom_pull(
            context=context, dag_id=self.dag.dag_id, task_ids='begin_dag'
        )

        self.entity.update({
            'date_offset': dt.strptime(date_upper_limit, '%Y-%m-%dT%H:%M:%S.%fZ')
        })

        metadata_writer = DatastoreMetadataWriter(self.project)
        metadata_writer.update(self.entity)
