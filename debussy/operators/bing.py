# -*- coding: utf-8 -*-

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from dags.debussy.hooks.bing import BingMapsHook

class BingMapsCreateJobOperator(BaseOperator):
    """Operator that takes a file from GCS, saves it locally at the data folder (in Composer, the data folder is GCSFuse mounted),
    and then sends it to the Bing Maps Batch Geocode Endpoint. 
    An HTTP connection must be provided with the URL and Bing Maps key previously configured.
    The operator is fixed expecting a PIPE delimited file, and it will always include the extra header indicating that the 2.0 version of the Geocode is to be used.
    Also, since the column header requires the use of "/" parameters in the name to match the requested names on the Bing Maps process, but it can happen that
    such a character is considered an invalid character for a DB column name, we hard-coded a replace of "_" to "/" to support this particular use-case.
    .. seealso::
        Follow a walkthrough of the Bing Maps Geocode Batch Job at:
        https://docs.microsoft.com/en-us/bingmaps/spatial-data-services/geocode-dataflow-api/geocode-dataflow-walkthrough
    
    :param gcs_file_path: field with the file to be downloaded from a GCS bucket. Only 1 file is currently supported.
        Should be in the form gs://bucket/path/file.extension (templated)
    :type gcs_file_path: str
    :param bing_maps_conn_id: name of the HTPP connection with the Bing Geocode endpoint and Key
    :type bing_maps_conn_id: str
    """
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

        # splitting the file path to extract the desired parts (which should be a path like gs://bucket/path/file.csv)
        file_parts = self.gcs_file_path.split('/')
        # gets the bucket
        bucket = file_parts[2]
        # getting the path to the file
        file_path = '/'.join(file_parts[3:-1])
        # getting the file name
        file_name = file_parts[-1]
        
        # setting the local path and preparing a "Ready" path for the prepared file
        local_file_path = '/home/airflow/gcs/data' + self.gcs_file_path[self.gcs_file_path.rindex('/'):]
        prepared_file_path = '/home/airflow/gcs/data/Ready_{}'.format(self.gcs_file_path[self.gcs_file_path.rindex('/')+1:])
        
        gcs_hook.download(bucket, '{}/{}'.format(file_path, file_name), local_file_path)

        # adding the version header
        # replacing the _ to / to conform to the required pattern of the Geocode Job
        with open(local_file_path, 'r') as rf:
            with open(prepared_file_path, 'w') as wf:
                wf.write('Bing Spatial Data Services, 2.0\n')
                for num, line in enumerate(rf, 1):
                    wf.write(line if num > 1 else line.replace('_', '/'))

        # preparing and making the call
        bm_hook = BingMapsHook(bing_maps_conn_id=self.bing_maps_conn_id)
        api_params = {
            'description': file_name,
            'input': 'pipe',
            'output': 'json'
        }
        response = bm_hook.call(method='', api_params=api_params, operation='POST', file_path=prepared_file_path)

        return response.json()

class BingMapsDownloadJobOperator(BaseOperator):
    """Operator that takes a BingMapsCreateJob operator, gets the Id of the job created and downloads the success file from the job.
    The file is then processed to remove the extra header (with the version of the Geocode) and replacing the column header's characters "/" for "_".
    This is to ensure that the column names can be processed as proper column names (BigQuery, for example, does not accept "/" in columns).
    Finally, the processed file is uploaded to the specified GCS bucket.
    .. seealso::
        Follow a walkthrough of the Bing Maps Geocode Batch Job at:
        https://docs.microsoft.com/en-us/bingmaps/spatial-data-services/geocode-dataflow-api/geocode-dataflow-walkthrough
    
    :param create_job_task: name of the BingMapsCreateJob to be downloaded. Must be in the same DAG/SubDag as this operator to work (templated)
    :type create_job_task: str
    :param gcs_file_path: field with the file name to be uploaded to a GCS bucket. Only 1 file is currently supported.
        Should be in the form gs://bucket/path/file.extension (templated)
    :type gcs_file_path: str
    :param bing_maps_conn_id: name of the HTPP connection with the Bing Geocode endpoint and Key
    :type bing_maps_conn_id: str
    """
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

        # splitting the file path to extract the desired parts (which should be a path like gs://bucket/path/file.csv)
        file_parts = self.gcs_file_path.split('/')
        # gets the bucket
        bucket = file_parts[2]
        # getting the path to the file
        file_path = '/'.join(file_parts[3:-1])
        # getting the file name
        file_name = file_parts[-1]
        
        # setting the local path with a "Pre" and preparing a processed path for the file
        local_file_path = '/home/airflow/gcs/data/Pre_{}'.format(file_name)
        prepared_file_path = '/home/airflow/gcs/data/{}'.format(file_name)
        
        # obtaining the Geocode job id
        task_instance = context['task_instance']
        create_resp = task_instance.xcom_pull(task_ids=self.create_job_task)
        job_id = create_resp['resourceSets'][0]['resources'][0]['id']

        # calling and downloading the file
        bm_hook = BingMapsHook(bing_maps_conn_id=self.bing_maps_conn_id)

        method = '{}/output/succeeded'.format(job_id)
        bm_hook.call(method=method, api_params={}, operation='GET', file_path=local_file_path)

        # processing the file and uploading to the bucket
        with open(local_file_path, 'r') as rf:
            with open(prepared_file_path, 'w') as wf:
                for num, line in enumerate(rf, 1):
                    if num == 1:
                        pass
                    elif num == 2:
                        wf.write(line.replace('/', '_'))
                    else:
                        wf.write(line)

        gcs_hook.upload(bucket, '{}/{}'.format(file_path, file_name), prepared_file_path)
