# -*- coding: utf-8 -*-

import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator


class ExtractorTemplateOperator(DataflowTemplateOperator):

    def __init__(self, project, config, task_id_sufix, parameters, *args, **kwargs):
        self.project = project
        self.config = config

        template_location = 'gs://{}/templates/{}/{}/v{}'.format(
            config['bucket_name'],
            config['template_name'],
            config['main_class'],
            config['template_version']
        )

        DataflowTemplateOperator.__init__(
            self,
            task_id='extract-{}'.format(task_id_sufix),
            template=template_location,
            parameters=parameters,
            poll_sleep=60,
            *args,
            **kwargs
        )

        def execute(self, context):
            DataflowTemplateOperator.execute(self, context)


class JDBCExtractorTemplateOperator(ExtractorTemplateOperator):
    def __init__(self, project, table, config, driver_class_name, db_parameters,
        bq_sink, *args, **kwargs):
        self.table = table

        task_id_sufix = 'table-{}'.format(self.table.lower())

        parameters = {
            'project': project,
            'driverClassName': driver_class_name,
            'query': "SELECT 1",
            'bigQuerySink': bq_sink
        }

        parameters.update(db_parameters)

        ExtractorTemplateOperator.__init__(
            self, project, config, task_id_sufix, parameters, *args, **kwargs
        )

    def execute(self, context):
        hook = GoogleCloudStorageHook()
        objs = hook.download(
            self.config['bucket_name'],
            '{}/{}.json'.format(self.config['schemas_raw_path'], self.table)
        )
        objs = json.loads(objs)

        fields = [obj['name'] for obj in objs]

        query = "SELECT {} FROM {}".format(', '.join(fields), self.table)
        self.parameters['query'] = query

        ExtractorTemplateOperator.execute(self, context)


class DatastoreExtractorTemplateOperator(ExtractorTemplateOperator):

    def __init__(self, project, namespace, kind, config, bq_sink, *args, **kwargs):
        self.namespace = namespace
        self.kind = kind

        kwargs.update({
            'task_id_sufix': '{}-{}'.format(
                self.namespace.lower(),
                self.kind.lower()
            ),
            'parameters': {
                'project': project,
                'sourceProjectId': config['sourceProjectId'],
                'namespace': self.namespace,
                'query': "SELECT 1",
                'bigQuerySink': bq_sink
            }
        })

        ExtractorTemplateOperator.__init__(self, project, config, *args, **kwargs)

    def execute(self, context):
        query = "SELECT * FROM {}".format(self.kind)
        self.parameters['query'] = query

        ExtractorTemplateOperator.execute(self, context)
