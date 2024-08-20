"""
This script defines an Invoker class intended for remote invocations of the Stimula API.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import sys

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

    def post_table(self, table, header, query, files, skiprows, insert, update, delete, execute, commit, format, deduplicate, post_script, context):



        if files and len(files) > 1:
            # post multiple files
            path = f"tables"

            # send filter as query parameter
            params = {'t': ','.join(table), 'h': header, 'insert': insert, 'update': update, 'delete': delete, 'execute': execute, 'commit': commit}

            # use table names as keys in file dictionary
            assert len(table) == len(files), "Provide exactly one file per table, not %s" % len(files)
            files = {table[i]: files[i] for i in range(len(table))}

            # return the token from json response
            return self.post_multi(path, params, files=files).text

        # post single table
        path = f"tables/{table}"

        if files and len(files) == 1:

            # post single file
            with files[0] as file:
                data = file.read()
        elif sys.stdin.isatty():
            # Input is being piped in
            data = sys.stdin.read()
        else:
            # no contents provided
            raise Exception('No contents provided, either use --file or -f flag, or pipe data to stdin.')

        # send filter as query parameter
        params = {"h": header, "q": query, 'skiprows': skiprows, 'insert': insert, 'update': update, 'delete': delete, 'execute': execute, 'commit': commit, 'deduplicate': deduplicate,
                  'style': format}

        # return the token from json response
        return self.post(path, params, data=data).text

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

    def post_multi(self, path, params=None, files=None):
        # create connection url
        url = f"{self._remote}/stimula/1.0/{path}"

        # set bearer token
        headers = {"Authorization": f"Bearer {self._token}"}

        # Send the POST request with the files dictionary
        response = requests.post(url, headers=headers, params=params, files=files)

        # if the response is not 200, raise an exception
        if response.status_code != 200:
            try:
                raise Exception(f"Request failed: {response.json()['msg']}\nRemote trace: {response.json()['trace']}")
            except:
                raise Exception(f"Request failed: {response.text}")

        return response
