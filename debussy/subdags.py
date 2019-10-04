# -*- coding: utf-8 -*-

from datetime import datetime as dt
from datetime import timedelta as td
from copy import deepcopy

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.executors.celery_executor import CeleryExecutor

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from dags.debussy.operators.basic import StartOperator, FinishOperator
from dags.debussy.operators.bigquery import BigQueryTableFlushOperator, BigQueryRawToClean, BigQueryExtractRowHashTable, BigQueryExtractOperationTable, BigQueryUpdateRowHashSourceTable
from dags.debussy.operators.extractors import JDBCExtractorTemplateOperator
from dags.debussy.operators.notification import SlackOperator
from dags.debussy.operators.python import MonthlyBranchPython


def _create_subdag(subdag_func, parent_dag, task_id, phase, default_args):
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag.dag_id, task_id),
        schedule_interval=None,
        default_args=default_args,
        catchup=False
    )

    begin_task = StartOperator(phase, trigger_rule='all_done', **default_args)
    end_task = FinishOperator(phase, trigger_rule='all_done', **default_args)
    subdag >> begin_task
    subdag >> end_task
    subdag_func(begin_task, end_task)

    return SubDagOperator(
        subdag=subdag,
        task_id=task_id,
        dag=parent_dag,
        executor=CeleryExecutor()
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

"""Subdag de controle de log de RowHash: este subdag é responsável por: fazer a exportação incremental das linhas alteradas
na tabela (NEW, UPDATE, DELETE). A exportação é feita primeiro em tabelas isoladas no dataset TEMP (para reduzir custos e o risco de conflitos de update).
Depois, a tabela é exportada para arquivos JSON no GCS. O processo então deve fazer um update na tabela original, indicando que as tais linhas
já foram processadas. Uma vez por mês, o processo também deve gerar um export FULL da tabela. Isso é feito para que um processo de reconstrução da tabela
tenha um ponto inicial relativamente próximo a partir do qual o log de mudanças deve ser aplicado."""
def create_rowhash_table_to_file_subdag(parent_dag, project_params, table_params, phase, default_args):
    project_id = project_params['project_id']
    env_level = project_params['env_level']
    table = table_params['table_id']

    def _internal(begin_task, end_task):
        extract_task_incremental = BigQueryExtractRowHashTable(
            project=project_id,
            env_level=env_level,
            table_params=table_params,
            **default_args
        )

        begin_task >> extract_task_incremental

        update_table = BigQueryUpdateRowHashSourceTable(
            project=project_id,
            env_level=env_level,
            table_params=table_params,
            **default_args
        )
        
        operation_tasks = {}
        for op in ['NEW', 'UPDATE', 'DELETE']:
            operation_tasks['table_' + op] = BigQueryExtractOperationTable(
                project=project_id,
                env_level=env_level,
                table_params=table_params,
                operation=op,
                **default_args
            )

            operation_tasks['export_' + op] = BigQueryToCloudStorageOperator(
                task_id='export_{}_{}'.format(op, table_params['table_id']),
                source_project_dataset_table='TEMP.{}_{}'.format(table_params['table_id'], op),
                destination_cloud_storage_uris='gs://{0}/{1}/{{{{ ts_nodash[:-5] }}}}/{1}_{{{{ ts_nodash[:-5] }}}}_{2}_*.json'.format(
                    project_params['rowhash_bucket'], table_params['table_id'], op
                ),
                export_format='NEWLINE_DELIMITED_JSON',
                **default_args
            )

            extract_task_incremental >> operation_tasks['table_' + op] >> operation_tasks['export_' + op] >> update_table
        
        checa_dia_mes = MonthlyBranchPython(
            true_result_task='{}_full'.format(table_params['table_id']),
            false_result_task='dummy_task',
            dia=table_params['dia'],
            **default_args
        )

        table_params_full = deepcopy(table_params)
        table_params_full.update({'full_load': True})

        extract_task_full = BigQueryExtractRowHashTable(
            project=project_id,
            env_level=env_level,
            table_params=table_params_full,
            **default_args
        )

        export_task_full = BigQueryToCloudStorageOperator(
            task_id='export_{}_full'.format(table_params['table_id']),
            source_project_dataset_table='TEMP.{}'.format(table_params['table_id']),
            destination_cloud_storage_uris='gs://{0}/{1}/{{{{ ts_nodash[:-5] }}}}/{1}_{{{{ ts_nodash[:-5] }}}}_FULL_*.json'.format(
                project_params['rowhash_bucket'], table_params['table_id']
            ),
            export_format='NEWLINE_DELIMITED_JSON',
            **default_args
        )

        update_table >> checa_dia_mes >> extract_task_full >> export_task_full >> end_task

        # apenas usado para que o branch operator não conecte direto com a task final
        # talvez possa ser removido?
        dummy_task = DummyOperator(
            task_id='dummy_task',
            **default_args
        )

        checa_dia_mes >> dummy_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        'table_{}_rowhash_subdag'.format(table),
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
