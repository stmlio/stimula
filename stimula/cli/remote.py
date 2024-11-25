"""
This script defines an Invoker class intended for remote invocations of the Stimula API.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import jwt
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

    def get_database_and_username(self, token):
        # decode token, without verifying the signature
        payload = jwt.decode(token, options={"verify_signature": False})
        # return database and username for easy re-authentication
        return payload['database'], payload['username']

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

    def post_table(self, tables, header, query, files, skiprows, insert, update, delete, execute, commit, format, post_script, context, substitutions):
        assert files is not None and len(files) > 0, 'Provide one or more files to post'

        assert len(tables) == len(files), "Provide exactly one file per table, not %s" % len(files)

        # post single file if there's only one table and no substitutions
        if len(files) == 1 and len(substitutions) == 0:
            # post single file from disk or stdin
            path = f"tables/{tables[0]}"

            # create query parameters
            params = {"h": header, "q": query, 'skiprows': skiprows, 'insert': insert, 'update': update, 'delete': delete, 'execute': execute, 'commit': commit,
                      'style': format}

            # return the result from json response
            return self.post(path, params=params, data=files[0]).json()

        else:
            # post multiple files
            path = f"tables"

            # send filter as query parameter
            params = {'t': ','.join(tables), 'h': header, 'insert': insert, 'update': update, 'delete': delete, 'execute': execute, 'commit': commit}

            # zip table names and files to create file dictionary for post request. Make sure the keys are unique
            file_map = {f'file{suffix}': (file_name, file, 'text/csv') for suffix, file_name, file in zip(range(len(files)), context, files)}

            if len(substitutions) == 1:
                # add substitutions files
                file_map['substitutions'] = ('substitutions.csv', substitutions[0], 'text/csv')

            # return the token from json response
            return self.post_multi(path, params, files=file_map).json()

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

        headers = {}

        # set bearer token
        if send_token:
            headers["Authorization"] = f"Bearer {self._token}"

        # post data to the url
        response = requests.post(url, headers=headers, params=params, data=data)

        # if the response is not 200, raise an exception
        if response.status_code != 200:
            self._raise_error(response)

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
            self._raise_error(response)

        return response

    def _raise_error(self, response):
        try:
            response_json = response.json()
        except:
            raise Exception(f"Request failed: {response.text}")
        raise Exception(f"Request failed: {response_json.get('msg')}\nRemote trace: {response_json.get('trace')}")
