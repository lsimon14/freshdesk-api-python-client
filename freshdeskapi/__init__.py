"""Freshdesk API Calls"""
import json
import os
from datetime import datetime
import re
from urllib3.util import Retry
import requests
import boto3

def upload_to_s3(s3_file, content):
    """Upload errors and exceptions to S3 bucket"""
    s3_client = boto3.client('s3')
    bucket = 'sf-kafka-logs'
    directory = 'EtlLogs/'
    with open(s3_file, 'w') as tempfile:
        tempfile.write(json.dumps(content))
    s3_client.upload_file(s3_file, bucket, directory + s3_file)
    os.remove(s3_file)

class Client:
    """Create Freshdesk API client and connect with JSON web token"""
    BASE_URL = 'https://achievementnetwork.freshdesk.com/api/v2/'

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        retries = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=['HEAD', 'GET', 'PUT', 'POST', 'DELETE', 'OPTIONS', 'TRACE'],
            backoff_factor=10)
        adapters = requests.adapters.HTTPAdapter(max_retries=retries)
        self.session.mount('https://', adapters)
        self.session.mount('http://', adapters)

    def request(self, method, path, params, datum=False):
        """Core request function"""
        url = f'{self.BASE_URL}{path}'
        auth = (self.api_key, '')
        response = self.session.request(auth=auth, method=method, url=url, params=params)
        if response.status_code not in [200, 201]:
            print(f'error with {method} request to {path}')
            upload_to_s3(
                s3_file=str(datetime.utcnow())+' freshdesk api error.json',
                content={
                    'method': method,
                    'url': url,
                    'status_code': response.status_code,
                    'response': response.content}
            )
            return False
        result = []
        if datum is True:
            return response.json()
        while True:
            try:
                url = re.search('<(.*)>', response.headers['link']).group(1)
                print(url)
                response = self.session.request(auth=auth, method=method, url=url)
                url = re.search('<(.*)>', response.headers['link']).group(1)
                result = result + response.json()
            except KeyError:
                result = result + response.json()
                break
        return result

    def get(self, path, params=None, datum=False):
        """Performs GET call to API"""
        if params is None:
            params = {}
        if datum is False:
            params.update({'page': 1, 'per_page': 100})
        return self.request(method='GET', path=path, params=params, datum=datum)

    def get_agent(self, agent_id):
        """Get agent"""
        path = f'agents/{agent_id}'
        return self.get(path=path, datum=True)

    def get_agents(self):
        """Get agents"""
        path = 'agents'
        return self.get(path=path)

    def get_company(self, company_id):
        """Get company"""
        path = f'companies/{company_id}'
        return self.get(path=path, datum=True)

    def get_companies(self):
        """Get companies"""
        path = 'companies'
        return self.get(path=path)

    def get_contact(self, contact_id):
        """Get contact"""
        path = f'contacts/{contact_id}'
        return self.get(path=path, datum=True)

    def get_contacts(self, updated_since=None, state=None):
        """Get contacts"""
        path = 'contacts'
        params = {'_updated_since': updated_since, 'state': state}
        return self.get(path=path, params=params)

    def get_ratings(self, created_since=None):
        """Get satisfaction ratings"""
        path = 'surveys/satisfaction_ratings'
        params = {'created_since': created_since}
        return self.get(path=path, params=params)

    def get_ticket(self, ticket_id):
        """Get ticket"""
        path = f'tickets/{ticket_id}'
        return self.get(path=path, datum=True)

    def get_tickets(self, updated_since=None):
        """Get tickets"""
        path = 'tickets'
        params = {'updated_since': updated_since}
        return self.get(path=path, params=params)
