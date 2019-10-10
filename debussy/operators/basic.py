# -*- coding: utf-8 -*-

from datetime import datetime as dt

from airflow.models import BaseOperator


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


class FinishOperator(BasicOperator):

    def __init__(self, phase, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='end',
            *args,
            **kwargs
        )
