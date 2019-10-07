# -*- coding: utf-8 -*-

import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator


class ExtractorTemplateOperator(DataflowTemplateOperator):

    def __init__(self, project, config, task_id_sufix, parameters, *args, **kwargs):
        template_location = 'gs://{}/templates/{}/v{}'.format(
            config['bucket_name'],
            config['template_name'],
            config['template_version']
        )

        parameters['project'] = project

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


class QueryExtractorTemplateOperator(ExtractorTemplateOperator):

    def __init__(self, project, config, task_id_sufix, parameters, query, *args,
        **kwargs):
        self.query = query

        ExtractorTemplateOperator.__init__(
            self, project, config, task_id_sufix, parameters, *args, **kwargs
        )

    def execute(self, context):
        self.parameters['query'] = self.query

        ExtractorTemplateOperator.execute(self, context)


# class JDBCExtractorTemplateOperator(QueryExtractorTemplateOperator):
class JDBCExtractorTemplateOperator(ExtractorTemplateOperator):
    def __init__(self, project, config, table, driver_class_name, db_parameters,
        bq_sink, *args, **kwargs):
        self.table = table

        task_id_sufix = 'table-{}'.format(self.table.lower())

        parameters = {
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


class DatastoreExtractorTemplateOperator(QueryExtractorTemplateOperator):

    def __init__(self, project, config, namespace, kind, condition, bq_sink, *args,
        **kwargs):
        self.namespace = namespace
        self.kind = kind
        self.condition = condition

        task_id_sufix = 'namespace-{}-kind-{}'.format(
            self.namespace.lower(),
            self.kind.lower()
        )

        parameters = {
            'sourceProjectId': config['sourceProjectId'],
            'namespace': self.namespace,
            'query': "SELECT 1",
            'bigQuerySink': bq_sink
        }

        query = "SELECT {} FROM {} WHERE {}"

        QueryExtractorTemplateOperator.__init__(
            self, project, config, task_id_sufix, parameters, query, *args, **kwargs
        )

    def execute(self, context):
        hook = GoogleCloudStorageHook()
        objs = hook.download(
            self.config['bucket_name'],
            '{}/{}_{}.json'.format(
                self.config['schemas_raw_path'], self.namespace, self.kind
            )
        )
        objs = json.loads(objs)
        fields = [obj['name'] for obj in objs]

        self.query = self.query.format(fields, self.namespace, self.kind)
        print()
        print(self.query)
        print()

        QueryExtractorTemplateOperator.execute(self, context)
