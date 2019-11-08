# -*- coding: utf-8 -*-

from datetime import datetime as dt
import logging
import time

from airflow.exceptions import AirflowTaskTimeout
from airflow.models import BaseOperator
from airflow.models import Variable


class BasicOperator(BaseOperator):

    def __init__(self, phase, step, *args, **kwargs):
        BaseOperator.__init__(
            self,
            task_id='{}_{}'.format(step, phase),
            *args,
            **kwargs
        )

        self.phase = phase
        self.step = step

    def execute(self, context):
        now = dt.utcnow()
        return now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


class StartOperator(BasicOperator):

    def __init__(self, phase, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='begin',
            *args,
            **kwargs
        )

    def execute(self, context):
        return BasicOperator.execute(self, context)


class FinishOperator(BasicOperator):

    def __init__(self, phase, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='end',
            *args,
            **kwargs
        )

    def execute(self, context):
        return BasicOperator.execute(self, context)
