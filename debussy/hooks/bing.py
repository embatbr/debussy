# -*- coding: utf-8 -*-

import requests
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

class BingMapsHook(BaseHook):
    """Hook for the Bing Maps Geocode operators.
    The hook expects an HTTP Airflow connection to be created, with the host set as "http://spatial.virtualearth.net/REST/v1/dataflows/geocode",
    and the password set as the user's Bing Maps key. The hook supports calling endpoints as GET (to check status and download results) and as POST (to create jobs).
    .. seealso::
        Follow a walkthrough of the Bing Maps Geocode Batch Job at:
        https://docs.microsoft.com/en-us/bingmaps/spatial-data-services/geocode-dataflow-api/geocode-dataflow-walkthrough
    
    :param bing_maps_conn_id: name of the HTPP connection with the Bing Geocode endpoint and Key
    :type bing_maps_conn_id: str
    """
    def __init__(self, bing_maps_conn_id='bing_maps_default'):
        self.conn = self.get_connection(bing_maps_conn_id)
    
    @staticmethod
    def is_downloadable(response):
        """Checks if the response contains a downloadable file, by verifiying the content-type.
        A json and html types are ignored and anything else (should only be text) is downloadable
        """
        header = response.headers
        content_type = header.get('content-type')
        if content_type.lower() in ('json', 'html'):
            return False
        return True

    def call(self, method, api_params, operation, file_path=None):
        """Method for calling the Geocode endpoint to create, check or download a job
        :param method": the method (e.g. job_id) that will be appended to the base URL
        "type method: str
        :param api_params: a dict with the query string parameters to be included.
            If no parameters are needed, supply an empty dict. The Bing Maps key is
            automatically added by the method so at least an empty dict is expected.
        :type api_params: dict
        :param operation: which operation is to be executed with the call (only GET and POST are currently accepted)
        :type operation: str
        :param file_path: when posting, indicates the location of the local file to be sent.
            When getting, indicates where the file should be written to.
            Can be ignored when checking status
        :type file_path: str
        """
        url = '{}/{}'.format(self.conn.host, method)
        api_params['key'] = self.conn.password
        
        if operation == 'GET':
            response = requests.get(url, params=api_params)

            if response and file_path:
                open(file_path, 'wb').write(response.content)
        elif operation == 'POST':
            with open(file_path,'r') as r:
                content = r.read()

            response = requests.post(url, data=content.encode('utf-8'), params=api_params, headers={'Content-Type': 'text/plain'})
        else:
            raise AirflowException('Currently unsupported call operation: {}'.format(operation))
        
        # when a file will be downloaded, we simply output the file_path (to prevent the entire document to be outputted)
        # else, we can safely output the response json
        if file_path:
            logging.info('File: {}'.format(file_path))
        else:
            logging.info(response.json())

        if response:
            return response
        else:
            msg = "Bing Maps API call failed ({})".format(response.json())
            raise AirflowException(msg)
