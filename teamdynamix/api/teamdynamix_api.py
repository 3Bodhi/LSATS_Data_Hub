import json
from re import search
import requests
import copy
import time
from datetime import datetime

from requests.exceptions import JSONDecodeError


def create_headers(api_token):
    return {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }
class TeamDynamixAPI:
    def __init__(self, base_url, app_id, headers):
        self.base_url = base_url
        self.app_id = app_id
        self.headers = headers

    def get(self, url_suffix):
        url = f'{self.base_url}/{self.app_id}/{url_suffix}'
        response = requests.get(url, headers=self.headers)
        return self._handle_response(response)

    def post(self, url_suffix, data):
        url = f'{self.base_url}/{self.app_id}/{url_suffix}'
        response = requests.post(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def put(self, url_suffix, data):
        url = f'{self.base_url}/{self.app_id}/{url_suffix}'
        response = requests.put(url, json=data, headers=self.headers)
        return self._handle_response(response)

    def _handle_response(self, response):
            if response.status_code == 200:
                print(f"{response.status_code} | Successful Request!")
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    return None
            elif response.status_code == 201:
                print(f"{response.status_code} | Successful Post!")
                return response.json()
            elif response.status_code == 204:
                print(f"{response.status_code} | Successful Post!")
                return None
            elif response.status_code == 429:
                reset_time = response.headers.get('X-RateLimit-Reset')
                if reset_time:
                    reset_time = datetime.strptime(reset_time, '%a, %d %b %Y %H:%M:%S %Z')
                    sleep_time = (reset_time - datetime.utcnow()).total_seconds() + 5
                    print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
                    time.sleep(sleep_time)
                    return self._retry_request(response.request)
            else:
                print(f"Failed: {response.status_code}")
                print(response.text)
                return None

    def _retry_request(self, request):
        method = request.method.lower()
        url = request.url
        data = request.body
        headers = request.headers

        if method == 'get':
            response = requests.get(url, headers=headers)
        elif method == 'post':
            response = requests.post(url, data=data, headers=headers)
        elif method == 'put':
            response = requests.put(url, data=data, headers=headers)
        else:
            raise ValueError(f"Unsupported method: {method}")

        return self._handle_response(response)
