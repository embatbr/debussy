# -*- coding: utf-8 -*-

from datetime import datetime

from airflow.operators.python_operator import BranchPythonOperator

def check_month_day(ds, **kwargs):
    date = datetime.strptime(ds, '%Y-%m-%d')

    if(date.day == kwargs['day']):
        return kwargs['true_result_task']
    else:
        return kwargs['false_result_task']

class MonthlyBranchPython(BranchPythonOperator):
    """Operator that simplifies cases when some branch must be executed once a month.
    Be careful that, since we only check the day, informing a number outside of the expected range
    will not fail the task, but the true branch will simply never be executed.
    :param day: the day when the check must return true
    :type day: int
    :param true_result_task: the name of the task that should be returned if the day matches
    :type true_result_task: str
    :param false_result_task: the name of the task that should be returned if the day doesn't match
    :type false_result_task: str
    """
    def __init__(self, day, true_result_task, false_result_task, *args, **kwargs):
        
        BranchPythonOperator.__init__(
            self,
            task_id='check_month_day{}'.format(day),
            python_callable=check_month_day,
            provide_context=True,
            op_kwargs={
                'day': day,
                'true_result_task': true_result_task,
                'false_result_task': false_result_task
            },
            *args,
            **kwargs
        )
    
    @property
    def operation(self):
        return 'check_month_day'
    
    def execute(self, context):
        BranchPythonOperator.execute(self, context)