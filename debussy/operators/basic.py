# -*- coding: utf-8 -*-

from datetime import datetime as dt
import logging
import time

from airflow.exceptions import AirflowTaskTimeout
from airflow.models import BaseOperator
from airflow.models import Variable


class BasicOperator(BaseOperator):

    def __init__(self, phase, step, given_now=None, *args, **kwargs):
        BaseOperator.__init__(
            self,
            task_id='{}_{}'.format(step, phase),
            *args,
            **kwargs
        )

        self.phase = phase
        self.step = step
        self.given_now = given_now

    def execute(self, context):
        if self.given_now:
            now = self.given_now
        else:
            now = dt.utcnow()
        return now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


class StartOperator(BasicOperator):

    def __init__(self, phase, given_now=None, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='begin',
            given_now=given_now,
            *args,
            **kwargs
        )

    def execute(self, context):
        return BasicOperator.execute(self, context)


class FinishOperator(BasicOperator):

    def __init__(self, phase, given_now=None, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='end',
            given_now=given_now,
            *args,
            **kwargs
        )

    def execute(self, context):
        return BasicOperator.execute(self, context)
