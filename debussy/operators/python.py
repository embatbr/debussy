# -*- coding: utf-8 -*-

from datetime import datetime

from airflow.operators.python_operator import BranchPythonOperator

def checa_dia_mes(ds, **kwargs):
    date = datetime.strptime(ds, '%Y-%m-%d')

    if(date.day == kwargs['dia']):
        return kwargs['true_result_task']
    else:
        return kwargs['false_result_task']

class MonthlyBranchPython(BranchPythonOperator):
    """Operador facilitador para checagens mensais. Como existem diversos processos que devem ser executados em um determinado dia do mês,
    montamos este operador para que não fosse necessário um módulo de métodos auxiliares (pelo menos para este caso)."""
    def __init__(self, dia, true_result_task, false_result_task, *args, **kwargs):
        
        BranchPythonOperator.__init__(
            self,
            task_id='checa_dia_mes_{}'.format(dia),
            python_callable=checa_dia_mes,
            provide_context=True,
            op_kwargs={
                'dia': dia,
                'true_result_task': true_result_task,
                'false_result_task': false_result_task
            },
            *args,
            **kwargs
        )
    
    @property
    def operation(self):
        return 'checa_dia_mes'
    
    def execute(self, context):
        BranchPythonOperator.execute(self, context)