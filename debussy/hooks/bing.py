# -*- coding: utf-8 -*-

import requests
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

class BingMapsHook(BaseHook):
    def __init__(self, bing_maps_conn_id='bing_maps_default'):
        self.conn = self.get_connection(bing_maps_conn_id)
    
    @staticmethod
    def is_downloadable(response):
        """
        Does the url contain a downloadable resource
        """
        header = response.headers
        content_type = header.get('content-type')
        if 'json' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False
        return True

    def call(self, method, api_params, operation, file_path=None):
        url = '{}/{}'.format(self.conn.host, method)
        api_params['key'] = self.conn.password
        
        if(operation == 'GET'):
            response = requests.get(url, params=api_params)

            if(self.is_downloadable(response)):
                open(file_path, 'wb').write(response.content)
        elif(operation == 'POST'):
            with open(file_path,'r') as r:
                content = r.read()

            response = requests.post(url, data=content.encode('utf-8'), params=api_params, headers={'Content-Type': 'text/plain'})
        else:
            raise AirflowException('Currently unsupported call operation: {}'.format(operation))
        
        logging.info('File: {}'.format(file_path) if(self.is_downloadable(response)) else response.json())
        if response:
            return response
        else:
            msg = "Bing Maps API call failed ({})".format(response.json())
            raise AirflowException(msg)
            