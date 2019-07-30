# -*- coding: utf-8 -*-

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
