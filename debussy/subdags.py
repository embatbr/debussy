# -*- coding: utf-8 -*-

from datetime import datetime as dt
from datetime import timedelta as td

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

from dags.debussy.operators.basic import StartOperator, FinishOperator
from dags.debussy.operators.bigquery import BigQueryTableFlushOperator, BigQueryRawToClean
from dags.debussy.operators.extractors import JDBCExtractorTemplateOperator
from dags.debussy.operators.notification import SlackOperator


def _create_subdag(subdag_func, parent_dag, task_id, phase, default_args):
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag.dag_id, task_id),
        schedule_interval=None,
        default_args=default_args,
        catchup=False
    )

    begin_task = StartOperator(phase, **default_args)
    end_task = FinishOperator(phase, **default_args)
    subdag >> begin_task
    subdag >> end_task
    subdag_func(begin_task, end_task)

    return SubDagOperator(
        subdag=subdag,
        task_id=task_id,
        dag=parent_dag
    )


def create_notification_subdag(parent_dag, env_level, phase, message, default_args):
    def _internal(begin_task, end_task):
        notification_task = SlackOperator(env_level, phase, message, **default_args)
        begin_task >> notification_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        '{}_notification_subdag'.format(phase),
        phase,
        default_args
    )


def create_sqlserver_bigquery_mirror_subdag(parent_dag, project_params, db_conn_data, conversor_wrapper,
    default_args):
    project_id = project_params['project_id']
    env_level = project_params['env_level']
    table = project_params['table']
    config = project_params['config']
    dataset_origin = project_params['dataset_origin']
    pools = project_params['pools']

    def _internal(begin_task, end_task):
        extractor_task = JDBCExtractorTemplateOperator(
            project=project_id,
            env_level=env_level,
            table=table,
            config=config,
            driver_class_name='com.microsoft.sqlserver.jdbc.SQLServerDriver',
            db_conn_data=db_conn_data,
            bq_sink='{}_RAW_{}.{}'.format(
                env_level.upper(),
                dataset_origin,
                table
            ),
            pool=pools['extractor'],
            **default_args
        )

        bq_flush_task = BigQueryTableFlushOperator(
            project=project_id,
            env_level=env_level,
            table=table,
            config=config,
            target_table_path='{}.{}_CLEAN_{}.{}'.format(
                project_id,
                env_level.upper(),
                dataset_origin,
                table
            ),
            pool=pools['bigquery'],
            **default_args
        )

        raw2clean_task = BigQueryRawToClean(
            project=project_id,
            env_level=env_level,
            table=table,
            config=config,
            target_table_path='{}.{}_CLEAN_{}.{}'.format(
                project_id,
                env_level.upper(),
                dataset_origin,
                table
            ),
            source_table_path='{}.{}_RAW_{}.{}'.format(
                project_id,
                env_level.upper(),
                dataset_origin,
                table
            ),
            conversor_wrapper=conversor_wrapper,
            pool=pools['bigquery'],
            **default_args
        )

        begin_task >> extractor_task >> bq_flush_task >> raw2clean_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        'table_{}_mirror_subdag'.format(table.lower()),
        table,
        default_args
    )


def create_datastore_bigquery_mirror_subdag(parent_dag, project_params, conversor_wrapper, default_args):
    project_id = project_params['project_id']
    env_level = project_params['env_level']
    table = project_params['table']
    config = project_params['config']
    dataset_origin = project_params['dataset_origin']
    pools = project_params['pools']

    def _internal(begin_task, end_task):
        # TODO chamar operador de extract via dataflow template

        bq_flush_task = BigQueryTableFlushOperator(
            project=project_id,
            env_level=env_level,
            table=table,
            config=config,
            target_table_path='{}.CLEAN_{}.{}'.format(
                project_id,
                dataset_origin,
                table
            ),
            pool=pools['bigquery'],
            **default_args
        )

        raw2clean_task = BigQueryRawToClean(
            project=project_id,
            env_level=env_level,
            table=table,
            config=config,
            target_table_path='{}.CLEAN_{}.{}'.format(
                project_id,
                dataset_origin,
                table
            ),
            source_table_path='{}.RAW_{}.{}'.format(
                project_id,
                dataset_origin,
                table
            ),
            conversor_wrapper=conversor_wrapper,
            pool=pools['bigquery'],
            **default_args
        )

        begin_task >> bq_flush_task >> raw2clean_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        'kind_{}_mirror_subdag'.format(table.lower()),
        table,
        default_args
    )
