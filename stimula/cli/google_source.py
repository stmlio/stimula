'''
This module provides file sources to read data from.
'''
import os
import pickle
import re

import gspread
import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from stimula.cli.file_source import StmlEvaluator

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def google_authenticate(credentials_file: str):
    # get folder name
    assert os.path.exists(credentials_file), f'Credentials file {credentials_file} not found in folder {os.getcwd()}. Please provide a valid Google credentials file. Browse to https://console.cloud.google.com/apis/credentials to create one.'
    flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
    creds = flow.run_local_server(port=8080)

    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)


class GoogleSource:
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id

    def google_get_credentials(self):
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first time.
        assert os.path.exists('token.pickle'), 'No token.pickle file found. Authenticate first.'
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

        assert creds, 'File token.pickle found but no valid credentials available. Authenticate first.'
        # If there are no valid credentials available, let the user log in.
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

        return creds

    def read_files(self, file_names, table_names, context):
        creds = self.google_get_credentials()
        client = gspread.authorize(creds)
        # open the spreadsheet
        sheet = client.open_by_key(self.sheet_id)
        # create map from sheet name to sheet
        all_sheets = {ws.title: ws for ws in sheet.worksheets()}
        # get file_names that do not exist in all_sheets
        missing_files = set(file_names) - set(all_sheets.keys())
        assert not missing_files, f"Sheet(s) {', '.join(missing_files)} not found in spreadsheet."
        # get all worksheets
        sheets = [all_sheets[file_name] for file_name in file_names]
        # read contents of each sheet into a dataframe
        sheets_as_df = [pd.DataFrame(ws.get_all_values()) for ws in sheets]

        # derive table names from file names if not provided
        table_names = table_names or [self._table_name_from_file_name(file_name) for file_name in file_names]
        # derive context from file names if not provided
        context = context or file_names

        # instantiate STML evaluator with lambda to read file from disk
        stml_evaluator = StmlEvaluator(lambda _, source_file_name: self._read_file(all_sheets, source_file_name))

        # replace STML files with their source
        file_contents, table_names, context, substitutions = stml_evaluator.replace_stmls_with_sources(file_names, sheets_as_df, table_names, context)

        return [file_contents, table_names, context, substitutions]

    def _table_name_from_file_name(self, file_name):
        # remove all characters after the first character that is not a letter or digit, or underscore
        return re.sub(r'[^a-zA-Z0-9_].*$', '', file_name)

    def _read_file(self, all_sheets, source_file_name):
        values = all_sheets[source_file_name].get_all_values()
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
