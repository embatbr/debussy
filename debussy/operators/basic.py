# -*- coding: utf-8 -*-

from airflow.models import BaseOperator


class BasicOperator(BaseOperator):

    def __init__(self, phase, step, function, *args, **kwargs):
        BaseOperator.__init__(
            self,
            task_id='{}_{}'.format(step, phase),
            *args,
            **kwargs
        )

        self.phase = phase
        self.step = step

        self.function = function

    def execute(self, context):
        self.function(context)


class StartOperator(BasicOperator):

    def __init__(self, phase, function, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='begin',
            function=function,
            *args,
            **kwargs
        )


class FinishOperator(BasicOperator):

    def __init__(self, phase, function, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='end',
            function=function,
            *args,
            **kwargs
        )
