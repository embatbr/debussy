# -*- coding: utf-8 -*-

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from dags.debussy.hooks.bing import BingMapsHook

class BingMapsJobSensor(BaseSensorOperator):
    """Operator that takes a BingMapsCreateJob operator, gets the Id of the job created and polls the job status from the Geocode endpoint.
    .. seealso::
        Follow a walkthrough of the Bing Maps Geocode Batch Job at:
        https://docs.microsoft.com/en-us/bingmaps/spatial-data-services/geocode-dataflow-api/geocode-dataflow-walkthrough
    
    :param create_job_task: name of the BingMapsCreateJob operator. Must be in the same DAG/SubDag as this operator to work (templated)
    :type create_job_task: str
    :param bing_maps_conn_id: name of the HTPP connection with the Bing Geocode endpoint and Key
    :type bing_maps_conn_id: str
    """
    template_fields = ('create_job_task', )

    @apply_defaults
    def __init__(self, create_job_task, bing_maps_conn_id='bing_maps_default', *args, **kwargs):
        self.task_id = self.operation
        self.create_job_task = create_job_task
        self.bing_maps_conn_id = bing_maps_conn_id

        BaseSensorOperator.__init__(self, task_id=self.operation, *args, **kwargs)

    @property
    def operation(self):
        return 'bing_maps_wait_job'

    def poke(self, context):
        # obtaining the Geocode job id
        task_instance = context['task_instance']
        create_resp = task_instance.xcom_pull(task_ids=self.create_job_task)
        job_id = create_resp['resourceSets'][0]['resources'][0]['id']

        # calling the endpoint to get the job status
        bm_hook = BingMapsHook(bing_maps_conn_id=self.bing_maps_conn_id)
        response = bm_hook.call(method=job_id, operation='GET', api_params={'output': 'json'})
        
        # if the response was successfull
        if response:
            # check the status of the job - Aborted is a failure case, pending is waiting and Completed is a success
            status = response.json()['resourceSets'][0]['resources'][0]['status']
            if(status == 'Completed'):
                return True
            elif(status == 'Pending'):
                return False
            else:
                raise AirflowException('Geocode job was aborted: {}'.format(response.json()['resourceSets'][0]['resources'][0]['errorMessage']))
        else:
            raise AirflowException('Geocode job encountered an error: {}'.format(response.json()['resourceSets'][0]['resources'][0]['errorMessage']))