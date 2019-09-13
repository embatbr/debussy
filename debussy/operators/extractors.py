# -*- coding: utf-8 -*-

import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator


class ExtractorTemplateOperator(DataflowTemplateOperator):

    def __init__(self, project, env_level, config, task_id_sufix, parameters, *args, **kwargs):
        self.project = project
        self.env_level = env_level
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
    # TODO remove any code "locked" to SQL Server

    def __init__(self, project, env_level, table, config, driver_class_name, db_conn_data,
        bq_sink, *args, **kwargs):
        self.table = table

        kwargs.update({
            'task_id_sufix': 'table-{}'.format(self.table.lower()),
            'parameters': {
                'project': project,
                'driverClassName': driver_class_name,
                'jdbcUrl': 'jdbc:sqlserver://{host}:{port};databaseName={dbname}'.format(
                    **db_conn_data
                ),
                'username': db_conn_data['user'],
                'password': db_conn_data['password'],
                'query': "SELECT 1",
                'bigQuerySink': bq_sink
            }
        })

        ExtractorTemplateOperator.__init__(self, project, env_level, config, *args, **kwargs)

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
