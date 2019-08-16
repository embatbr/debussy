# -*- coding: utf-8 -*-

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook


class SlackOperator(SlackWebhookOperator):

    SLACK_CONN_ID = 'slack_id'

    def __init__(self, env_level, name, slack_msg='', slack_conn_id=None, *args, **kwargs):
        self.slack_conn_id = slack_conn_id or self.SLACK_CONN_ID

        SlackWebhookOperator.__init__(
            self,
            task_id='slack_{}_alert'.format(name),
            http_conn_id=self.slack_conn_id,
            username='airflow',
            *args,
            **kwargs
        )

        self.env_level = env_level
        self.slack_msg = slack_msg

    def execute(self, context):
        self.webhook_token = BaseHook.get_connection(self.slack_conn_id).password

        self.message = """
            *[{env_level}]*
            *Dag:* {dag}
            *Task:* {task}
            *Scheduled:* {scheduler_exec_date}

            {message}

            *Log Url:* {log_url}
            """.format(
                env_level=self.env_level,
                dag=context.get('task_instance').dag_id,
                task=context.get('task_instance').task_id,
                scheduler_exec_date=context.get('execution_date'),
                message=self.slack_msg,
                log_url=context.get('task_instance').log_url,
            )

        SlackWebhookOperator.execute(self, context)
