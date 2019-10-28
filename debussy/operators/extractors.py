# -*- coding: utf-8 -*-

import json

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator


class ExtractorTemplateOperator(DataflowTemplateOperator):

    def __init__(self, project, config, task_id_sufix, parameters, *args, **kwargs):
        self.config = config

        template_location = 'gs://{}/templates/{}/v{}'.format(
            self.config['bucket_name'],
            self.config['template_name'],
            self.config['template_version']
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

class JDBCExtractorTemplateCustomQueryOperator(ExtractorTemplateOperator):
    template_fields = ('sql_template_params', )

    @apply_defaults
    def __init__(self, project, config, table, driver_class_name, db_parameters,
        bq_sink, query, sql_template_params, build_query, *args, **kwargs):
        self.table = table
        self.query = query
        self.sql_template_params = sql_template_params
        self.build_query = build_query(self)

        task_id_sufix = 'table-{}'.format(self.table.lower())

        self.parameters = {
            'driverClassName': driver_class_name,
            'query': "SELECT 1",
            'bigQuerySink': bq_sink
        }
        
        self.parameters.update(db_parameters)

        ExtractorTemplateOperator.__init__(
            self, project, config, task_id_sufix, self.parameters, *args, **kwargs
        )

    def execute(self, context):
        self.parameters['query'] = self.build_query(context).format(**self.sql_template_params)
        
        ExtractorTemplateOperator.execute(self, context)


class DatastoreExtractorTemplateOperator(QueryExtractorTemplateOperator):

    def __init__(self, project, config, namespace, kind, build_query, bq_sink,
        *args, **kwargs):
        self.namespace = namespace
        self.kind = kind
        self.build_query = build_query(self)

        task_id_sufix = 'namespace-{}-kind-{}'.format(
            self.namespace.lower(),
            self.kind.lower()
        )

        parameters = {
            'sourceProjectId': config['sourceProjectId'],
            'namespace': self.namespace,
            'bigQuerySink': bq_sink,
            'whiteListString': ''
        }

        query = "SELECT 1"

        QueryExtractorTemplateOperator.__init__(
            self, project, config, task_id_sufix, parameters, query, *args, **kwargs
        )

    def execute(self, context):
        self.query = self.build_query(context)

        entity = self.xcom_pull(
            context=context,
            dag_id=self.dag.dag_id,
            task_ids='datastore_get_{}_{}'.format(self.namespace, self.kind)
        )

        white_list_string = entity.get('fields', '')
        self.parameters['whiteListString'] = white_list_string

        QueryExtractorTemplateOperator.execute(self, context)
