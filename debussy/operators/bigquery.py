# -*- coding: utf-8 -*-

import json

from copy import deepcopy

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow import AirflowException
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

class BigQueryDropTableOperator(BigQueryOperator):
    """Classe que força o BigQuery a deletar uma tabela.
    É usado nos casos onde o dataset de destino possui dias de expiração.
    Como o operador padrão do BigQuery no Airflow não possui opção para sobrescrever este valor e
    a definição de tabela destino (mesmo com WRITE_TRUNCATE) não reinicia o número de dias para a expiração,
    isso faz com que o job tenha erros a cada ciclo de expiração de tabela."""

    def __init__(self, project_id, dataset_id, table_id, *args, **kwargs):
        self.task_id='drop-table-{}.{}'.format(dataset_id, table_id)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
    
    @property
    def operation(self):
        return 'drop_table'

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)
        
        # Remover a tabela caso exista
        full_table_name = '%s.%s.%s' % (self.project_id, self.dataset_id, self.table_id)
        bq_hook.get_conn().cursor().run_table_delete(full_table_name, ignore_if_missing=True)

class BigQueryTableOperator(BigQueryOperator):

    def __init__(self, project, table, sql_template_params, task_id=None, *args, **kwargs):
        self.project = project
        self.table = table
        self.sql_template_params = sql_template_params

        BigQueryOperator.__init__(
            self,
            task_id=task_id if task_id else '{}-table-{}'.format(self.operation, self.table),
            sql='SELECT 1',
            allow_large_results=True,
            use_legacy_sql=False,
            *args,
            **kwargs
        )

    def execute(self, context):
        self.sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
        BigQueryOperator.execute(self, context)


class BigQueryTableDeleteDataOperator(BigQueryTableOperator):

    SQL_TEMPLATE = """DELETE
FROM
    `{target_table_path}`
WHERE
    {where_condition}
"""

    def __init__(self, project, table, target_table_path, where_condition, *args, **kwargs):
        sql_template_params = {
            'target_table_path': target_table_path,
            'where_condition': where_condition
        }

        BigQueryTableOperator.__init__(
            self,
            project=project,
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


class BigQueryTableFlushOperator(BigQueryTableDeleteDataOperator):

    def __init__(self, project, table, config, target_table_path, *args, **kwargs):
        BigQueryTableDeleteDataOperator.__init__(
            self,
            project=project,
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
        BigQueryTableDeleteDataOperator.execute(self, context)


class BigQueryRawToClean(BigQueryTableOperator):

    SQL_TEMPLATE = """INSERT INTO
    `{target_table_path}`
SELECT
    {source_table_fields_converted}
FROM
    `{source_table_path}`
"""

    def __init__(self, project, table, config, target_table_path, source_table_path,
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
            table=table,
            sql_template_params=sql_template_params,
            write_disposition='WRITE_TRUNCATE', # point of attention
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