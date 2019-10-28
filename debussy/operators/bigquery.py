# -*- coding: utf-8 -*-

import json

from copy import deepcopy

from dags.debussy.helper import json_traverser

from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator, BigQueryCreateEmptyTableOperator

from airflow import AirflowException
from airflow.models import BaseOperator

class BigQueryDropTableOperator(BaseOperator):
    """Operator that drops a table in BigQuery. Should only be used before v1.10.3 of Airflow, when
    an official operator was included. This one will always ignore if the table is not found.
    :param project_id: the project id
    :type project_id: str
    :param dataset_id: the dataset name
    :type dataset_id: str
    :param table_id: the table name
    :type table_id: str
    :param bigquery_conn_id: the name of the Airflow BQ connection to be used
    :type bigquery_conn_id: str
    """

    def __init__(self, project_id, dataset_id, table_id, bigquery_conn_id='bigquery_default', *args, **kwargs):
        self.task_id='drop-table-{}.{}'.format(dataset_id, table_id)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bigquery_conn_id = bigquery_conn_id

        BaseOperator.__init__(
            self,
            task_id=self.task_id,
            *args,
            **kwargs
        )

    @property
    def operation(self):
        return 'drop_table'

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)

        # Remover a tabela caso exista
        full_table_name = '%s.%s.%s' % (self.project_id, self.dataset_id, self.table_id)
        bq_hook.get_conn().cursor().run_table_delete(full_table_name, ignore_if_missing=True)

class BigQueryTableOperator(BigQueryOperator):

    def __init__(self, project, table, sql_template_params, task_id=None, sql=None, *args, **kwargs):
        self.project = project
        self.table = table
        self.sql_template_params = sql_template_params

        BigQueryOperator.__init__(
            self,
            task_id=task_id if task_id else '{}-table-{}'.format(self.operation, self.table),
            sql=sql if sql else 'SELECT 1',
            allow_large_results=True,
            use_legacy_sql=False,
            *args,
            **kwargs
        )

    def execute(self, context):
        try:
            self.sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
        except AttributeError:
            self.sql = self.sql.format(**self.sql_template_params)
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
            where_condition="true",
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
            # write_disposition='WRITE_APPEND',
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

class BigQueryMergeTableOperator(BigQueryTableOperator):
    """Operator that creates and executes a MERGE statement in BigQuery.
    The statement, by default, always does an updated when matched and an
    insert when not. No variation of this structure are currently supported.
    The source and destination tables must have the same schemas (or, at least,
    all columns of the destination table must exist in the source table).
    There must be some PK field list with which the tables can be merged.
    Also, a list of fields to be ignored in the insert and updated portions, and
    entire records can be ignored or just specific fields.
    :param pk_fields: the PK fields as a list, which will make up the join condition
    :type pk_fields: list|str
    ;param source_table: the complete table to be used as a source (project.dataset.table)
    :type source_table: str
    ;param destination_table: the complete table to be used as the destination (project.dataset.table)
    :type destination_table: str
    :param table_schema: the table schema definition as a list of dictionaries
    :type table_schema: list|dict
    :param update_fields_ignore: the list of fields to be ignored in the update clause.
        The PK fields are always excluded from this clause as they are meaningless here
        The list can include record fields or subrecord fields like:
        ['field1', 'RECORD', 'RECORD2.field2']
    :type update_fields_ignore: list|str
    :param insert_fields_ignore: the list of fields to be ignored in the update clause.
        The list can only include record fields but not subrecord ones (since BQ won't allow partial RECORD insertions) like:
        ['field1', 'RECORD']
    :type insert_fields_ignore: list|str
    """
    SQL_TEMPLATE="""MERGE `{destination_table}` AS d
    USING
    (
        SELECT
            *
        FROM
            `{source_table}`
    ) AS s
    ON
        {join_clause}
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT(
        {insert_list}
    ) VALUES (
        {insert_list}
    )
    """
    @apply_defaults
    def __init__(
        self,
        pk_fields,
        source_table,
        destination_table,
        table_schema,
        update_fields_ignore=[],
        insert_fields_ignore=[],
        *args,
        **kwargs
    ):
        # preparing the update clause
        # the helper function allows us to sequentially traverse the fields
        # including nested record to filter out any exceptions that came with the ignore argument
        update_clause = ''
        for f, l in json_traverser(table_schema):
            # constructing a complete path so that we can ignore specific fields or subfields
            full_field_name = f['name'] if l == '' else '{}.{}'.format(l, f['name'])

            # ignore whole records, specific fields/subfields, pk fields
            if(l not in update_fields_ignore and full_field_name not in update_fields_ignore and full_field_name not in pk_fields):
                update_clause += 'd.{0} = s.{0},\n'.format(full_field_name)
        update_clause = update_clause[:-2]

        # preparing the insert clause
        # only whole records or root fields can be referenced here
        # so a simple pass through the list is enough
        insert_list = ''
        for f in table_schema:
            if(f['name'] not in insert_fields_ignore):
                insert_list += f['name'] + ',\n'
        insert_list = insert_list[:-2]

        # preparing the join clause
        join_clause = ''
        for f in pk_fields:
            join_clause += 's.{0} = d.{0} AND\n'.format(f)
        join_clause = join_clause[:-5]

        sql_template_params = {
            'destination_table': destination_table,
            'source_table': source_table,
            'join_clause': join_clause,
            'update_clause': update_clause,
            'insert_list': insert_list
        }

        # using the destination table as the base, we split it to supply the required params for the parent operator
        project, dataset, table = destination_table.split('.')
        BigQueryTableOperator.__init__(
            self,
            project=project,
            table=table,
            sql_template_params=sql_template_params,
            *args, **kwargs
        )

    def execute(self, context):
        BigQueryTableOperator.execute(self, context)

class BigQueryDropCreateTableOperator(BigQueryDropTableOperator, BigQueryCreateEmptyTableOperator):
    def __init__(self, project_id, dataset_id, table_id, schema_fields, *args, **kwargs):
        BigQueryDropTableOperator.__init__(
            self, 
            project_id=project_id, 
            dataset_id=dataset_id, 
            table_id=table_id, 
            *args, **kwargs
        )
     
        BigQueryCreateEmptyTableOperator.__init__(
            self, 
            task_id='drop_create_{}'.format(table_id),
            project_id=project_id, 
            dataset_id=dataset_id, 
            table_id=table_id, 
            schema_fields=schema_fields, 
            *args, **kwargs
        )
    
    @property
    def operation(self):
        return 'drop_create_table'

    def execute(self, context):
        BigQueryDropTableOperator.execute(self, context)

        BigQueryCreateEmptyTableOperator.execute(self, context)

class BigQueryGetMaxFieldOperator(BaseOperator):
    SQL_TEMPLATE = """
SELECT
    {max_field} AS value
FROM
    `{project_id}.{dataset_id}.{table_id}`
    """
    def __init__(
        self,
        project_id,
        dataset_id,
        table_id,
        field_id,
        field_type,
        format_string=None,
        timezone=None,
        bigquery_conn_id='bigquery_default', 
        delegate_to=None,
        *args,
        **kwargs
    ):
        if field_type == 'TIMESTAMP':
            max_field = 'FORMAT_TIMESTAMP("{}", MAX({}), "{}")'.format(
                format_string if format_string else '%FT%T%z',
                field_id,
                timezone if timezone else 'UTC'
            )
        elif field_type == 'DATETIME':
            max_field = 'FORMAT_DATETIME("{}", MAX({}))'.format(
                format_string if format_string else '%FT%T',
                field_id
            )
        elif field_type == 'DATE':
            max_field = 'FORMAT_DATE(("{}", MAX({}))'.format(
                format_string if format_string else '%F',
                field_id
            )
        elif field_type == 'TIME':
            max_field = 'FORMAT_TIME(("{}", MAX({}))'.format(
                format_string if format_string else '%T',
                field_id
            )
        elif field_type in ['FLOAT64', 'NUMERIC']:
            max_field = 'FORMAT("{}", MAX({}))'.format(
                format_string if format_string else '%f',
                field_id
            )
        elif field_type == 'INT64':
            max_field = 'FORMAT("{}", MAX({}))'.format(
                format_string if format_string else '%d',
                field_id
            )
        elif field_type == 'BOOLEAN':
            max_field = 'CAST(MAX({}) AS STRING)'.format(
                field_id
            )
        elif field_type == 'BYTES':
            max_field = 'MAX(TO_BASE64({}))'.format(
                field_id
            )
        else:
            raise AirflowException('Unsupported type {} in BigQueryGetMaxFieldOperator'.format(field_type))

        self.sql_template_params = {
            'project_id': project_id,
            'dataset_id': dataset_id,
            'table_id': table_id,
            'max_field': max_field
        }

        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

        BaseOperator.__init__(self, *args, **kwargs)
    
    @property
    def operation(self):
        return 'get_max_value'

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, delegate_to=self.delegate_to, use_legacy_sql=False)
        bq_cursor = bq_hook.get_conn().cursor()
        sql = self.SQL_TEMPLATE.format(**self.sql_template_params)
        bq_cursor.execute(sql)
        result = bq_cursor.fetchall()
        return result[0][0]