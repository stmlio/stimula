import base64
import re

import requests

from stimula.stml.model import Attribute

'''
This class reads documents from remote APIs.
'''

AFAS_ENCODING = {
    '/': '_2F',
    '#': '_23',
    '&': '_26',
    ':': '_3A',
    '?': '_3F',
    '*': '_2A',
    '<': '_3C',
    '>': '_3E',
    '%': '_25',
    '+': '_2B',
    '~': '_7E',
    '-': '_2D',
    '@': '_40',
    '!': '_21',
    '$': '_24',
    '_': '_5F',
    '\'': '_27',
}


class ApiReader:
    def read_document(self, attribute: Attribute, params):
        # if calling the AFAS API, then convert the name to an AFAS compatible name
        if attribute.api == 'afas' and 'name' in params:
            params['name'] = self._convert_afas_file_name(params['name'])

        # get url, expand placeholders with values from row
        url = attribute.url.format(**params)

        # create authorization header
        headers = {"Authorization": attribute.auth} if attribute.auth else {}

        # send http GET request to url
        response = requests.get(url, headers=headers)

        # assert that the response status code is 200
        assert response.status_code == 200, f"API call to {url} failed with status code {response.status_code}"

        # check if the response is an AFAS document
        if self._is_afas_document(response):
            # extract afas document from response
            return self._extract_afas_document(response, params)

        # return response content
        return response.content

    def _is_afas_document(self, response):
        # check if the response is an AFAS document
        return 'application/json' in response.headers.get('Content-Type', '') and 'filedata' in response.json()

    def _extract_afas_document(self, response, params):
        #     extract json from response
        response_json = response.json()

        # verify document name
        assert 'filename' in response_json, "AFAS response does not contain a filename"

        # get filename from response
        filename = response_json['filename']

        # get expected name from params
        expected_name = params['name']

        assert filename == expected_name, f"AFAS document name {filename} does not match expected name {expected_name}"
        assert 'filedata' in response_json, "AFAS response does not contain filedata"

        # decode filedata from base64
        document = base64.b64decode(response_json['filedata'])

        # return document
        return document

    def _convert_afas_file_name(self, name):
        if name:
            # replace special characters with their afas encoding
            afas_name = re.sub(r"[/#&:?*<>%+~\-@!$_']", lambda m: AFAS_ENCODING[m.group()], name)
            return afas_name

        return ''
