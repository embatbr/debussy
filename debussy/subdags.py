# -*- coding: utf-8 -*-

from datetime import datetime as dt
from datetime import timedelta as td

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

from dags.debussy.operators.basic import StartOperator, FinishOperator
from dags.debussy.operators.bigquery import BigQueryTableFlushOperator, BigQueryRawToClean
from dags.debussy.operators.datastore import DatastoreGetObjectOperator
from dags.debussy.operators.datastore import DatastorePutObjectOperator
from dags.debussy.operators.extractors import DatastoreExtractorTemplateOperator
from dags.debussy.operators.extractors import JDBCExtractorTemplateOperator
from dags.debussy.operators.notification import SlackOperator


# INTERNALS BEGIN


def _create_subdag(subdag_func, parent_dag, task_id, phase, default_args):
    """Creates a subdag, initiated by a StartOperator and ended by a FinishOperator.
    Also, uses an internal function, passed as argument, to create the other subdag
    operators in between.
    """
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag.dag_id, task_id),
        schedule_interval=None,
        default_args=default_args,
        catchup=False
    )

    begin_task = StartOperator(phase, **default_args)
    subdag >> begin_task
    end_task = FinishOperator(phase, **default_args)
    subdag >> end_task
    subdag_func(begin_task, end_task)

    return SubDagOperator(
        subdag=subdag,
        task_id=task_id,
        dag=parent_dag
    )


def _create_bigquery_mirror_subdag(parent_dag, project_params, extractor_class,
    conversor_wrapper, task_id_format, default_args):
    """Creates a subdag to mirror a table from a given source to BigQuery.
    """
    project_id = project_params['project_id']
    config = project_params['config']

    metadata_params_get = dict(project_params['metadata_params_get'])
    metadata_params_put = dict(project_params['metadata_params_put'])

    extractor_params = dict(project_params['extract'])
    extractor_params['project'] = project_id
    extractor_params['config'] = config
    extractor_params.update(default_args)

    bigquery_params = dict(project_params['bigquery'])
    bigquery_pool = bigquery_params['pool']
    bigquery_table = bigquery_params['table']
    raw_table_name = bigquery_params['raw_table_name']
    clean_table_name = bigquery_params['clean_table_name']

    def _internal(begin_task, end_task):
        bq_flush_task = BigQueryTableFlushOperator(
            project=project_id,
            config=config,
            table=bigquery_table,
            target_table_path=raw_table_name,
            pool=bigquery_pool,
            **default_args
        )

        datastore_get_task = DatastoreGetObjectOperator(**metadata_params_get)

        extract_task = extractor_class(**extractor_params)

        raw2clean_task = BigQueryRawToClean(
            project=project_id,
            config=config,
            table=bigquery_table,
            target_table_path=clean_table_name,
            source_table_path=raw_table_name,
            conversor_wrapper=conversor_wrapper,
            pool=bigquery_pool,
            **default_args
        )

        datastore_put_task = DatastorePutObjectOperator(**metadata_params_put)

        begin_task >> bq_flush_task >> datastore_get_task >> extract_task
        extract_task >> raw2clean_task >> datastore_put_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        task_id_format.format(bigquery_table),
        bigquery_table,
        default_args
    )


# INTERNALS END

# EXTERNALS BEGIN


def create_notification_subdag(parent_dag, env_level, phase, message, default_args):
    """Creates a subdag that notifies a given message in a slack channel.
    """
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


def create_sqlserver_bigquery_mirror_subdag(parent_dag, project_params, conversor_wrapper,
    default_args):
    return _create_bigquery_mirror_subdag(
        parent_dag,
        project_params,
        JDBCExtractorTemplateOperator,
        conversor_wrapper,
        'table_{}_mirror_subdag',
        default_args
    )


def create_datastore_bigquery_mirror_subdag(parent_dag, project_params, conversor_wrapper,
    default_args):
    return _create_bigquery_mirror_subdag(
        parent_dag,
        project_params,
        DatastoreExtractorTemplateOperator,
        conversor_wrapper,
        '{}_mirror_subdag',
        default_args
    )


# EXTERNALS END
