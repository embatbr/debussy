# -*- coding: utf-8 -*-

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from dags.debussy.hooks.bing import BingMapsHook

class BingMapsCreateJob(BaseOperator):
    template_fields = ('gcs_file_path', )

    @apply_defaults
    def __init__(self, gcs_file_path, bing_maps_conn_id='bing_maps_default', *args, **kwargs):
        self.gcs_file_path = gcs_file_path
        self.bing_maps_conn_id = bing_maps_conn_id
        
        BaseOperator.__init__(self, task_id=self.operation, *args, **kwargs)
    
    @property
    def operation(self):
        return 'bing_maps_create_job'

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook()

        file_parts = self.gcs_file_path.split('/')
        bucket = file_parts[2]
        file_path = '/'.join(file_parts[3:-1])
        file_name = file_parts[-1]
        
        local_file_path = '/home/airflow/gcs/data' + self.gcs_file_path[self.gcs_file_path.rindex('/'):]
        prepared_file_path = '/home/airflow/gcs/data/Ready_{}'.format(self.gcs_file_path[self.gcs_file_path.rindex('/')+1:])
        
        gcs_hook.download(bucket, '{}/{}'.format(file_path, file_name), local_file_path)

        with open(local_file_path, 'r') as rf:
            with open(prepared_file_path, 'w') as wf:
                wf.write('Bing Spatial Data Services, 2.0\n')
                for num, line in enumerate(rf, 1):
                    wf.write(line if num > 1 else line.replace('_', '/'))

        bm_hook = BingMapsHook(bing_maps_conn_id=self.bing_maps_conn_id)
        api_params = {
            'description': file_name,
            'input': 'pipe',
            'output': 'json'
        }
        response = bm_hook.call(method='', api_params=api_params, operation='POST', file_path=prepared_file_path)

        return response.json()

class BingMapsDownloadJob(BaseOperator):
    template_fields = ('create_job_task', 'gcs_file_path')

    @apply_defaults
    def __init__(
        self, 
        create_job_task, 
        gcs_file_path, 
        bing_maps_conn_id='bing_maps_default', 
        *args, **kwargs
    ):
        self.gcs_file_path = gcs_file_path
        self.create_job_task = create_job_task
        self.bing_maps_conn_id = bing_maps_conn_id
        
        BaseOperator.__init__(self, task_id=self.operation, *args, **kwargs)

    @property
    def operation(self):
        return 'bing_maps_download_job'
    
    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook()

        file_parts = self.gcs_file_path.split('/')
        bucket = file_parts[2]
        file_path = '/'.join(file_parts[3:-1])
        file_name = file_parts[-1]
        
        local_file_path = '/home/airflow/gcs/data/Pre_{}'.format(file_name)
        prepared_file_path = '/home/airflow/gcs/data/{}'.format(file_name)
        
        task_instance = context['task_instance']
        create_resp = task_instance.xcom_pull(task_ids=self.create_job_task)
        job_id = create_resp['resourceSets'][0]['resources'][0]['id']

        bm_hook = BingMapsHook(bing_maps_conn_id=self.bing_maps_conn_id)

        method = '{}/output/succeeded'.format(job_id)
        bm_hook.call(method=method, api_params={}, operation='GET', file_path=local_file_path)

        with open(local_file_path, 'r') as rf:
            with open(prepared_file_path, 'w') as wf:
                for num, line in enumerate(rf, 1):
                    if(num == 1):
                        pass
                    elif(num == 2):
                        wf.write(line.replace('/', '_'))
                    else:
                        wf.write(line)

        gcs_hook.upload(bucket, '{}/{}'.format(file_path, file_name), prepared_file_path)