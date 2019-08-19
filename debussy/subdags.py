# -*- coding: utf-8 -*-

from datetime import datetime as dt
from datetime import timedelta as td

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator

from dags.debussy.operators.basic import StartOperator, FinishOperator
from dags.debussy.operators.notification import SlackOperator


def _create_subdag(subdag_func, parent_dag, task_id, default_args):
    subdag = DAG(
        dag_id='{}.{}'.format(parent_dag.dag_id, task_id),
        schedule_interval=None,
        default_args=default_args,
        catchup=False
    )

    subdag_func(subdag)

    return SubDagOperator(
        subdag=subdag,
        task_id=task_id,
        dag=parent_dag
    )


def notification_subdag(parent_dag, env_level, phase, message, default_args):
    def _internal(subdag):
        begin_task = StartOperator(phase, **default_args)
        end_task = FinishOperator(phase, **default_args)

        notification_task = SlackOperator(env_level, phase, message, **default_args)

        subdag >> begin_task >> notification_task >> end_task

    return _create_subdag(
        _internal,
        parent_dag,
        '{}_notification_subdag'.format(phase),
        default_args
    )
