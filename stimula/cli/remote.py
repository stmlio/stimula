"""
This script defines an Invoker class intended for remote invocations of the Stimula API.

Author: Romke Jonker
Email: romke@rnadesign.net
"""

import requests


class Invoker:
    def __init__(self, remote):
        self._remote = remote

    def set_context(self, token):
        # save token for consecutive requests
        self._token = token

    def auth(self, database, username, password):
        # create connection url
        path = 'auth'

        # post credentials as form data
        data = {"database": database, "username": username, "password": password}

        # return the token from json response
        return self.post(path, data=data, send_token=False).json()['token']

    def list(self, filter):
        # set path
        path = 'tables'

        # send filter as query parameter
        params = {"q": filter}

        # return the token from json response
        return self.get(path, params).json()

    def mapping(self, filter):
        # create connection url
        path = f"tables/{filter}/header"

        # send filter as query parameter
        params = {"style": "csv"}

        # return the token from json response
        return self.get(path, params).text

    def count(self, table, header, query):
        # create connection url
        path = f"tables/{table}/count"

        # send filter as query parameter
        params = {"h": header, "q": query}

        # return the token from json response
        return self.get(path, params).json()['count']

    def get_table(self, table, header, query):
        path = f"tables/{table}"

        # send filter as query parameter
        params = {"h": header, "q": query}

        # return the token from json response
        return self.get(path, params).text

    def post_table(self, table, header, query, contents, skiprows, insert, update, delete, execute, commit, format, deduplicate, post_script, context):
        path = f"tables/{table}"

        # send filter as query parameter
        params = {"h": header, "q": query, 'skiprows': skiprows, 'insert': insert, 'update': update, 'delete': delete, 'execute': execute, 'commit': commit, 'deduplicate': deduplicate,
                  'style': format}

        # return the token from json response
        return self.post(path, params, contents).text

    def get(self, path, params):
        # create connection url
        url = f"{self._remote}/stimula/1.0/{path}"

        # set bearer token
        headers = {"Authorization": f"Bearer {self._token}"}

        # get table list from the url
        response = requests.get(url, headers=headers, params=params)

        # if the response is not 200, raise an exception
        if response.status_code != 200:
            raise Exception(f"Request failed: {response.json()['msg']}\nRemote trace: {response.json()['trace']}")

        # return the response
        return response

    def post(self, path, params=None, data=None, send_token=True):
        # create connection url
        url = f"{self._remote}/stimula/1.0/{path}"

        # set bearer token
        if send_token:
            headers = {"Authorization": f"Bearer {self._token}"}
        else:
            headers = {}

        # post data to the url
        response = requests.post(url, headers=headers, params=params, data=data)

        # if the response is not 200, raise an exception
        if response.status_code != 200:
            try:
                raise Exception(f"Request failed: {response.json()['msg']}\nRemote trace: {response.json()['trace']}")
            except:
                raise Exception(f"Request failed: {response.text}")

        return response
