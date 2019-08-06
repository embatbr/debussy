# -*- coding: utf-8 -*-

import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


class BigQueryTableOperator(BigQueryOperator):

    def __init__(self, project, env_level, table, sql_template_params, *args, **kwargs):
        self.project = project
        self.env_level = env_level#.upper()
        self.table = table
        self.sql_template_params = sql_template_params

        BigQueryOperator.__init__(
            self,
            task_id='{}-table-{}'.format(self.operation, self.table),
            sql='SELECT 1', # None is not allowed, what makes no sense
            allow_large_results=True,
            use_legacy_sql=False,
            *args,
            **kwargs
        )

    def execute(self, context):
        self.sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
        BigQueryOperator.execute(self, context)


class BigQueryTableDeleteOperator(BigQueryTableOperator):

    SQL_TEMPLATE = """DELETE
FROM
    {target_table_path}
WHERE
    {where_condition}
"""

    def __init__(self, project, env_level, table, target_table_path, where_condition, *args, **kwargs):
        sql_template_params = {
            'target_table_path': target_table_path,
            'where_condition': where_condition
        }

        BigQueryTableOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            table=table,
            sql_template_params=sql_template_params,
            *args,
            **kwargs
        )

    @property
    def operation(self):
        return 'delete'

    def execute(self, context):
        BigQueryTableOperator.execute(self, context)


class BigQueryTableFlushOperator(BigQueryTableDeleteOperator):

    def __init__(self, project, env_level, table, target_table_path, *args, **kwargs):
        BigQueryTableDeleteOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            table=table,
            target_table_path=target_table_path,
            where_condition="1 = 1",
            *args,
            **kwargs
        )

    @property
    def operation(self):
        return 'flush'

    def execute(self, context):
        BigQueryTableDeleteOperator.execute(self, context)


class BigQueryRawToClean(BigQueryTableOperator):

    SQL_TEMPLATE = """INSERT INTO
    {target_table_path}
SELECT
    {source_table_fields_converted}
FROM
    {source_table_path}
"""

    def __init__(self, project, env_level, table, config, target_table_path, source_table_path,
        conversor_wrapper, *args, **kwargs):
        self.config = config
        self.conversor = conversor_wrapper(self)

        sql_template_params = {
            'target_table_path': target_table_path,
            'source_table_path': source_table_path
        }

        BigQueryTableOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            table=table,
            sql_template_params=sql_template_params,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_NEVER',
            *args,
            **kwargs
        )

    @property
    def schema(self):
        hook = GoogleCloudStorageHook()
        objs = hook.download(
            self.config['bucket_name'],
            '{}/{}.json'.format(self.config['schemas_clean_path'], self.table)
        )

        return json.loads(objs)

    @property
    def operation(self):
        return 'raw2clean'

    def execute(self, context):
        converted_fields = self.conversor(self.table)
        converted_fields = ",\n    ".join(converted_fields)
        self.sql_template_params['source_table_fields_converted'] = converted_fields

        BigQueryTableOperator.execute(self, context)
