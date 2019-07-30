# -*- coding: utf-8 -*-

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook


class SlackOperator(SlackWebhookOperator):

    SLACK_CONN_ID = 'slack_id'

    def __init__(self, name, slack_msg, slack_conn_id=None, *args, **kwargs):
        self.slack_conn_id = slack_conn_id or self.SLACK_CONN_ID
        slack_webhook_token = BaseHook.get_connection(self.slack_conn_id).password

        SlackWebhookOperator.__init__(
            self,
            task_id='slack_{}_alert'.format(name),
            http_conn_id=self.slack_conn_id,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow',
            *args,
            **kwargs
        )
