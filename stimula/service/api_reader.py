import requests

'''
This class reads documents from remote APIs.
'''


class ApiReader:
    def read_document(self, url_template, params):
        # get url, expand placeholders with values from row
        url = url_template.format(**params)

        # send http GET request to url
        response = requests.get(url)

        # assert that the response status code is 200
        assert response.status_code == 200, f"API call to {url} failed with status code {response.status_code}"

        return response.content
