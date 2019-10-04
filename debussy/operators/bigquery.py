# -*- coding: utf-8 -*-

import json

from copy import deepcopy

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from airflow import AirflowException
from airflow.contrib.operators.bigquery_operator import BigQueryOperator, BigQueryCreateEmptyTableOperator

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

    def __init__(self, project, env_level, table, sql_template_params, *args, **kwargs):
        self.project = project
        self.env_level = env_level
        self.table = table
        self.sql_template_params = sql_template_params

        BigQueryOperator.__init__(
            self,
            task_id='{}-table-{}'.format(self.operation, self.table),
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


class BigQueryTableFlushOperator(BigQueryTableDeleteDataOperator):

    def __init__(self, project, env_level, table, config, target_table_path, *args, **kwargs):
        BigQueryTableDeleteDataOperator.__init__(
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
        BigQueryTableDeleteDataOperator.execute(self, context)


class BigQueryRawToClean(BigQueryTableOperator):

    SQL_TEMPLATE = """INSERT INTO
    `{target_table_path}`
SELECT
    {source_table_fields_converted}
FROM
    `{source_table_path}`
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

class BigQueryUpdateRowHashSourceTable(BigQueryTableOperator):
    """Operador respnsável por fazer o update das linhas modificadas (guardadas na tabela da TEMP) de volta na tabela original.
    Por questão de custos nas tabelas anuais, montamos o update como um comando único para todas as operações.
    Caso a tabela não seja anual, basta executar o update contra a tabela final.
    Caso seja, devemos fazer um loop, para cada ano de 2000 a 2050 (e o default 1900) e executar o update tabela a tabela.
    Como o update cobra o preço de um select na tabela toda, precisamos passar uma única vez para reduzir custos."""
    SQL_TEMPLATE = """UPDATE `{source_table_path}` a
    SET a.METADATA.LastSentRowHash = 
        CASE operation
            WHEN 'NEW' THEN a.METADATA.RowHash
            WHEN 'UPDATE' THEN a.METADATA.RowHash
            WHEN 'DELETE' THEN NULL
        END
FROM
    (
        SELECT 'NEW' AS operation, {pk_list} FROM `{rowhash_table_path}_NEW`
        UNION ALL
        SELECT 'UPDATE' AS operation, {pk_list} FROM `{rowhash_table_path}_UPDATE`
        UNION ALL
        SELECT 'DELETE' AS operation, {pk_list} FROM `{rowhash_table_path}_DELETE`
    ) AS b
WHERE
    {join_clause}"""

    def __init__(self, project, env_level, table_params, *args, **kwargs):
        self.task_id = 'update_source_table_{}'.format(table_params['table_id'])
        
        # preparando o join entre as tabelas do update e a lista de campos na PK
        join_clause = ''
        pk_list = ''
        for f in table_params['pk_fields']:
            join_clause += 'a.{0} = b.{0} AND\n'.format(f)
            pk_list += f + ', '
        join_clause = join_clause[:-5]
        pk_list = pk_list[:-2]

        sql_template_params = {
            'join_clause': join_clause,
            'pk_list': pk_list,
            # caso a tabela seja anual, preparamos um placeholder YEAR que será preenchido durante a execução do operador
            'source_table_path': '{}.{}.{}{}'.format(
                project, table_params['dataset_id'], table_params['table_id'], '_YEAR' if 'partitioning' in table_params else ''
            ),
            'rowhash_table_path': '{}.TEMP.{}'.format(project, table_params['table_id'])
        }

        BigQueryTableOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            table=table_params['table_id'],
            sql_template_params=sql_template_params,
            *args,
            **kwargs
        )

        self.table_params = table_params
    
    @property
    def operation(self):
        return 'update_source_table'

    def execute(self, context):
        if('partitioning' in self.table_params):
            # guardando os comandos originais para que possamos reinicar após cada iteração
            sql_original = deepcopy(self.sql)
            source_table_original = self.sql_template_params['source_table_path']
            for year in list(range(2000,2051)) + [1900]:
                # preparando e executando a query
                self.sql_template_params['source_table_path'] = self.sql_template_params['source_table_path'].replace('YEAR', str(year))
                self.sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
                BigQueryTableOperator.execute(self, context)

                # voltando os valores originais para que a próxima iteração dê um replace corretamente
                self.sql = sql_original
                self.sql_template_params['source_table_path'] = source_table_original
        else:
            BigQueryTableOperator.execute(self, context)

class BigQueryExtractOperationTable(BigQueryDropTableOperator, BigQueryTableOperator):
    """Operador que extrai uma fotografia das linhas alteradas com base em uma operação (NEW, UPDATE, DELETE).
    O operador pressupõe um dataset TEMP para ser usado como base para a extração. Isso é feito para reduzir custos
    e evitar conflitos durante a execução do sistema de logs. Embora não seja uma garantia de imutabilidade, desde que o
    Airflow seja sempre usado no procesamento, nunca deve haver conflitos de update.
    O operador irá obter as linhas referentes a uma operação e gerar uma nova tabela apenas com essas linhas,
    para que essa possa ser exportada para o GCS de forma permanente."""
    SQL_TEMPLATE = """SELECT
    *
FROM
    `{source_table_path}`
WHERE
    {where_clause}"""
    def __init__(self, project, env_level, table_params, operation, *args, **kwargs):
        self.task_id = 'export_table_' + operation

        if(operation == "NEW"):
            selected_clause = "(METADATA.FlagDelete IS FALSE AND METADATA.LastSentRowHash IS NULL)"
        elif(operation == "UPDATE"):
            selected_clause = "(METADATA.FlagDelete IS FALSE AND METADATA.RowHash <> METADATA.LastSentRowHash)"
        elif(operation == "DELETE"):
            selected_clause = "(METADATA.FlagDelete IS TRUE AND METADATA.LastSentRowHash IS NOT NULL)"
        else:
            raise AirflowException('Unknown RowHash operation. Must be one of NEW, UPDATE, DELETE')
        
        sql_template_params = {
            'where_clause': selected_clause,
            'source_table_path': '{}.TEMP.{}'.format(project, table_params['table_id'])
        }

        table_id = table_params['table_id'] + '_' + operation

        BigQueryDropTableOperator.__init__(
            self,
            project_id=project,
            dataset_id='TEMP',
            table_id=table_id
        )

        BigQueryTableOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            dataset_id='TEMP',
            table=table_id,
            destination_dataset_table='{}.TEMP.{}_{}'.format(project, table_params['table_id'], operation),
            sql_template_params=sql_template_params,
            write_disposition='WRITE_TRUNCATE',
            *args,
            **kwargs
        )
    
    @property
    def operation(self):
        return 'rowhash_table_operation'

    def execute(self, context):
        BigQueryDropTableOperator.execute(self, context)

        BigQueryTableOperator.execute(self, context)

class BigQueryExtractRowHashTable(BigQueryDropTableOperator, BigQueryTableOperator):
    """Operador que prepara uma tabela para a conversão das linhas NEW/UPDATE/DELETE em arquivos.
    Esse processo gera um log, próximo ao de um banco de dados, de tal forma que as tabelas possam ser recriadas
    em outros ambientes a partir de seu log. O processo suporta cargas incrementais ou full."""
    SQL_TEMPLATE = """SELECT
    *
FROM
    `{source_table_path}`
WHERE
    {where_clause}"""

    def __init__(self, project, env_level, table_params, *args, **kwargs):
        FULL_LOAD = """TRUE"""

        INCREMENTAL_LOAD = """(METADATA.FlagDelete IS FALSE AND METADATA.LastSentRowHash IS NULL)
    OR (METADATA.FlagDelete IS FALSE AND METADATA.RowHash <> METADATA.LastSentRowHash)
    OR (METADATA.FlagDelete IS TRUE AND METADATA.LastSentRowHash IS NOT NULL)"""

        sql_template_params = {
            'where_clause': FULL_LOAD if 'full_load' in table_params else INCREMENTAL_LOAD,
            'source_table_path': '{}.{}.{}'.format(
                project, table_params['dataset_view_id'] if 'partitioning' in table_params else table_params['dataset_id'], table_params['table_id'])
        }

        BigQueryDropTableOperator.__init__(
            self,
            project_id=project,
            dataset_id='TEMP',
            table_id=table_params['table_id']
        )

        BigQueryTableOperator.__init__(
            self,
            project=project,
            env_level=env_level,
            dataset_id='TEMP',
            table=table_params['table_id'],
            destination_dataset_table='{}.TEMP.{}'.format(project, table_params['table_id']),
            sql_template_params=sql_template_params,
            write_disposition='WRITE_TRUNCATE',
            *args,
            **kwargs
        )

        self.task_id = table_params['table_id'] + ('_full' if 'full_load' in table_params else '_incremental')

    @property
    def operation(self):
        return 'extract_rowhash_table'

    def execute(self, context):
        BigQueryDropTableOperator.execute(self, context)

        BigQueryTableOperator.execute(self, context)