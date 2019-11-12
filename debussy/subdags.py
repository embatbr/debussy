# -*- coding: utf-8 -*-

# TODO MAKE ME A PACKAGE!

from datetime import datetime as dt
from datetime import timedelta as td
from copy import deepcopy

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.executors.celery_executor import CeleryExecutor

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator

from dags.debussy.operators.basic import StartOperator, FinishOperator
from dags.debussy.operators.bigquery import BigQueryTableFlushOperator, BigQueryRawToClean
from dags.debussy.operators.datastore import DatastoreGetObjectOperator
from dags.debussy.operators.datastore import DatastorePutObjectOperator
from dags.debussy.operators.extractors import DatastoreExtractorTemplateOperator
from dags.debussy.operators.extractors import JDBCExtractorTemplateOperator
from dags.debussy.operators.notification import SlackOperator
from dags.debussy.operators.python import MonthlyBranchPython


# INTERNALS BEGIN


def _create_subdag(subdag_func, parent_dag, task_id, phase, default_args,
    trigger_rule='all_success'):
    """Creates a subdag, initiated by a StartOperator and ended by a FinishOperator.
    Also, uses an internal function, passed as argument, to create the other subdag
    operators in between.
    """
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag.dag_id, task_id),
        schedule_interval=None,
        catchup=False,
        default_args=default_args
    )

    begin_task = StartOperator(phase, trigger_rule=trigger_rule, **default_args)
    subdag >> begin_task
    end_task = FinishOperator(phase, trigger_rule=trigger_rule, **default_args)
    subdag >> end_task
    subdag_func(begin_task, end_task)

    return SubDagOperator(
        subdag=subdag,
        task_id=task_id,
        dag=parent_dag,
        executor=CeleryExecutor()
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
        notification_task = SlackOperator(phase, message, **default_args)
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

def create_simple_pyspark_subdag(parent_dag, project_params, zone, region,
    storage_bucket, subnetwork_uri, main_file, extra_files, job_name, arguments,
    default_args):
    """Creates a simplified Dataproc cluster for processing a PySpark script.
    To make things as simple as possible, the cluster is fixed as a standard-2 master (no HA),
    4 standard-4 workers (2 of which preemptible), 200gb fixed disk size and GCS/BQ
    connectors (no other packages can be installed). The cluster auto-deletes with a 10min
    idle timeout, and only internal IPs. This subdag aims for a single, pure PySpark process,
    without much personalization and dependencies.
    :param parent_dag: the parent dag that this subdag will be associated
    :type parent_dag: DAG
    :param project_params: dict with project-wide configurations, such as project_id, dataset_id, etc
    :type project_params: dict
    :param zone: the GCP zone (e.g. us-central1-b) where the cluster will be created
    :type zone: str
    :param region: the GCP region (e.g. us-central1) where the cluster will be created
    :type region: str
    :param storage_bucket: the bucket where temporary data for the cluster will be stored
    :type storage_bucket: str
    :param subnetwork_uri: the full URI of the subnetwork where the Dataproc cluster will execute
        (e.g.: projects/project-id/regions/us-central1/subnetworks/subnet-id)
    :type subnetwork_uri: str
    :param main_file: the path of the file to be executed (should be in a GCS bucket that the cluster will have access to)
    :type main_file: str
    :param extra_files: the list of support files to be passed to the cluster (must be a Python file like .py or .egg)
    :type extra_files: list|str
    :param job_name: the name of the job that will be displayed in the cluster's jobs list
    :type job_name: str
    :param arguments: the list of arguments to be passed to the main file
    :type arguments: list|str
    """
    project_id = project_params['project_id']

    def _internal(begin_task, end_task):
        task_id = 'cluster-{}'.format(job_name)
        cluster_name = '{}-cluster'.format(job_name)

        cluster = DataprocClusterCreateOperator(
            task_id=task_id,
            project_id=project_id,
            zone=zone,
            cluster_name=cluster_name,
            num_workers=2,
            num_preemptible_workers=2,
            storage_bucket=storage_bucket,
            master_machine_type='n1-standard-2',
            master_disk_size=200,
            worker_machine_type='n1-standard-4',
            worker_disk_size=200,
            init_actions_uris=['gs://dataproc-initialization-actions/connectors/connectors.sh'],
            metadata={
                'gcs-connector-version': '1.9.16',
                'bigquery-connector-version': '0.13.16'
            },
            subnetwork_uri=subnetwork_uri,
            internal_ip_only=True,
            region=region,
            idle_delete_ttl=600,
            **default_args
        )

        job = DataProcPySparkOperator(
            task_id=job_name,
            main=main_file,
            arguments=arguments,
            pyfiles=extra_files,
            job_name=job_name,
            cluster_name=cluster_name,
            region=region,
            **default_args
        )

        begin_task >> cluster >> job >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        'pyspark-{}'.format(job_name),
        job_name,
        default_args,
        trigger_rule='all_done'
    )

# EXTERNALS END
