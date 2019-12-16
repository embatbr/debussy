# -*- coding: utf-8 -*-

from airflow.operators.mssql_operator import MsSqlOperator

class MsSqlTableOperator(MsSqlOperator):
    template_fields = ('sql_template_params', )
    
    def __init__(
        self,
        project,
        table,
        sql_template_params,
        mssql_conn_id,
        database,
        sql,
        build_query,
        autocommit=True,
        *args, **kwargs
    ):
        self.project = project
        self.table = table
        self.sql_template_params = sql_template_params
        self.build_query = build_query(self)

        MsSqlOperator.__init__(
            self,
            mssql_conn_id=mssql_conn_id,
            database=database,
            sql=sql,
            autocommit=autocommit,
            *args,
            **kwargs
        )

    def execute(self, context):
        self.sql = self.build_query(context).format(**self.sql_template_params)

        MsSqlOperator.execute(self, context)