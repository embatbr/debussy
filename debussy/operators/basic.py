# -*- coding: utf-8 -*-

from datetime import datetime as dt
import logging
import time

from airflow.exceptions import AirflowTaskTimeout
from airflow.models import BaseOperator
from airflow.models import Variable


class BasicOperator(BaseOperator):

    def __init__(self, phase, step, use_lock, *args, **kwargs):
        BaseOperator.__init__(
            self,
            task_id='{}_{}'.format(step, phase),
            *args,
            **kwargs
        )

        self.phase = phase
        self.step = step
        self.use_lock = use_lock

    def execute(self, context):
        now = dt.utcnow()
        return now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _get_lock(self):
        lock = Variable.get('lock_{}'.format(self.dag.dag_id))
        if lock == 'False':
            lock = False
        elif lock == 'True':
            lock = True

        return lock


class StartOperator(BasicOperator):

    def __init__(self, phase, use_lock=False, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='begin',
            use_lock=use_lock,
            *args,
            **kwargs
        )

    def execute(self, context):
        if self.use_lock:
            start_time = dt.now()
            lock = self._get_lock()
            while lock:
                now = dt.now()
                delta = now - start_time
                if delta.total_seconds() > 60*30: # 30 minutes
                    raise AirflowTaskTimeout()

                logging.info('lock: {}'.format(lock))
                logging.info('sleeping for 5 minutes')
                time.sleep(60*5)
                lock = self._get_lock()

            Variable.set('lock_{}'.format(self.dag.dag_id), True)
            logging.info('lock acquired')

        return BasicOperator.execute(self, context)


class FinishOperator(BasicOperator):

    def __init__(self, phase, use_lock=False, *args, **kwargs):
        BasicOperator.__init__(
            self,
            phase=phase,
            step='end',
            use_lock=use_lock,
            *args,
            **kwargs
        )

    def execute(self, context):
        if self.use_lock:
            Variable.set('lock_{}'.format(self.dag.dag_id), False)
            logging.info('lock released')

        return BasicOperator.execute(self, context)
