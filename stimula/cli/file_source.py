import io
import os
import re
import sys

import pandas as pd
from numpy import isnan


class FileSource:
    def read_files(self, file_names, table_names, context):
        '''
        Read file contents from disk or stdin. Returns three arrays with identical length: file contents, table names, and context.
        The file names in the result may be different from the input file names, as STML files are replaced with their source.
        '''

        # we may have a single file coming from stdin, or multiple files from disk/sheet
        number_of_files = len(file_names) if sys.stdin.isatty() else 1

        # verify we have either as many tables and contexts, or none if we can derive from STML or file name
        assert table_names is None or len(table_names) == number_of_files, f"Provide exactly one table per file, or none. not {len(table_names)}"
        assert context is None or len(context) == number_of_files, f"Provide exactly one table per file, or none. not {len(context)}"

        # raise error if no contents are provided
        assert (file_names and len(file_names) > 0) or not sys.stdin.isatty(), 'No contents provided, either use --file or -f flag, or pipe data to stdin.'

        if not sys.stdin.isatty():
            assert file_names is None or len(file_names) == 0, 'Do not provide files using --file or -f when piping data to stdin.'
            # Input is being piped in, assume single file
            # read from stdin as binary
            file_contents = [sys.stdin.buffer.read()]
            file_names = ['stdin']
            context = context or ['stdin']

        else:
            # read all files
            file_contents = [open(file_name, 'rb').read() for file_name in file_names]
            # derive table names from file names if not provided
            table_names = table_names or [self._table_name_from_file_name(file_name) for file_name in file_names]
            # derive context from file names if not provided
            context = context or file_names

        # read as df, no header line, don't use nan
        file_contents_as_df = [pd.read_csv(io.BytesIO(file_content), header=None, keep_default_na=False) for file_content in file_contents]

        # instantiate STML evaluator with lambda to read file from disk
        stml_evaluator = StmlEvaluator(lambda original_file_name, source_file_name: self._read_file(original_file_name, source_file_name))

        # replace STML files with their source
        file_contents, table_names, context, substitutions = stml_evaluator.replace_stmls_with_sources(file_names, file_contents_as_df, table_names, context)

        return file_contents, table_names, context, substitutions

    def _table_name_from_file_name(self, file_name):
        # remove path and extension
        base_name = os.path.basename(file_name)
        # remove all characters after the first character that is not a letter or digit, or underscore
        return re.sub(r'[^a-zA-Z0-9_].*$', '', base_name)

    def _read_file(self, original_file_name, source_file_name):
        # assume the source file is in the same folder as the original file
        folder = os.path.dirname(original_file_name)
        # concat folder and source file name
        full_file_name = os.path.join(folder, source_file_name)
        # read file into dataframe
        return pd.read_csv(full_file_name)


class StmlEvaluator:

    def __init__(self, file_reader_lambda):
        # lambda to read source files, so we can use source files from file and from google sheets
        self._file_reader_lambda = file_reader_lambda

    def replace_stmls_with_sources(self, file_names, file_contents_as_df, table_names, context):
        # iterate over all files
        result = [self.replace_stml_with_source(n, f, t, c) for n, f, t, c in zip(file_names, file_contents_as_df, table_names, context)]
        # unzip result to file_contents, table_names, context, substitutions
        return zip(*result)

    def replace_stml_with_source(self, file_name, df, table_name, context):
        # check if cell A1 starts with '@'
        if not df.iloc[0, 0].startswith('@'):
            # not an STML file, return as binary CSV string
            csv = df.to_csv(index=False, header=False).encode('utf-8')
            return csv, table_name, context, None

        # get source file name, target table name and optional substitutions file name
        source_file_name = self._get_source_file_name(df, file_name)
        target_table_name = self._get_target_table_name(df, file_name)
        substitutions_file_name = self._get_substitutions_file_name(df, file_name)

        # read source and target columns from STML file
        source_and_target_column_list = self._get_source_and_target_columns(df, file_name)

        # use lambda to read source file into dataframe. Also pass original source file name to hint folder (if on disk)
        source_df = self._file_reader_lambda(file_name, source_file_name)

        # replace header line with target headers
        source_df = self._replace_header_line(source_and_target_column_list, source_df)

        # convert to csv
        source_csv = source_df.to_csv(index=False).encode('utf-8')

        # read substitutions file if the file name is provided
        if substitutions_file_name:
            substitutions_df = self._file_reader_lambda(file_name, substitutions_file_name)
            # convert to csv
            substitutions_csv = substitutions_df.to_csv(index=False).encode('utf-8')
        else:
            substitutions_csv = None

        # return source file as csv, target table name, the source file name and the substitions file as context
        return [source_csv, target_table_name, source_file_name, substitutions_csv]

    def _get_source_file_name(self, df, file_name):
        # assert it contains '@source'
        assert '@source' in df.iloc[0, 0], f"File {file_name} is an STML file, but does not contain '@source' in A1."
        # get source file name from B1
        source_file_name = df.iloc[0, 1]
        # assert it's not empty
        assert source_file_name, f"File {file_name} is an STML file, but B1 is empty."

        return source_file_name

    def _get_target_table_name(self, df, file_name):
        # assert A2 contains '@target'
        assert '@target' in df.iloc[1, 0], f"File {file_name} is an STML file, but does not contain '@target' in B2."
        # read target file name from B2
        target_table_name = df.iloc[1, 1]
        # assert it's not empty
        assert target_table_name, f"File {file_name} is an STML file, but B2 is empty."
        return target_table_name

    def _get_substitutions_file_name(self, df, file_name):
        # checkk if there's a substitutions header
        if not '@substitutions' in df.iloc[2, 0] or not df.iloc[2, 1]:
            # no substitutions file, return None
            return None

        # get substitutions file name from B3
        substitutions_file_name = df.iloc[2, 1]

        return substitutions_file_name

    def _get_source_and_target_columns(self, df, file_name):
        assert any(df[0] == 'source_column'), f"File {file_name} is an STML file, but does not contain 'source_column' in column A."
        # find first row number that contains 'source_column' in column A
        source_columns_row = df[df[0] == 'source_column'].index[0]
        # get modifier keys
        modifier_keys = self._get_modifier_column_names(df, source_columns_row)
        # create list of source and target columns, including modifiers
        column_rows = df.iloc[source_columns_row + 1:, :]
        # iterate over rows in dataframe using iterrows
        source_and_target_column_list = [self._get_source_and_target(row, modifier_keys) for _, row in column_rows.iterrows()]
        return source_and_target_column_list

    def _get_modifier_column_names(self, df, source_columns_row):
        # get list of additional non-empty headers to treat as modifiers from the source_columns_row
        modifiers = df.iloc[source_columns_row, 2:].tolist()
        # supported modifiers
        known_modifiers = ['unique', 'skip', 'default-value', 'exp', 'deduplicate', 'table', 'name', 'qualifier', 'key']
        # list unknown modifiers
        unknown_modifiers = [modifier for modifier in modifiers if modifier and modifier not in known_modifiers and modifier]
        # assert no unknown modifiers
        assert not unknown_modifiers, f'Unknown modifiers: {unknown_modifiers}'
        return modifiers

    def _get_source_and_target(self, row, modifiers_keys):
        source = row[0]
        target = row[1]
        # get columns 2 and up, with length equal to modifiers
        modifier_values = row[2:2 + len(modifiers_keys)]
        # create list of modifiers=value, where value is not empty
        modifier_list = list(map(lambda kv: kv[0] + '=' + kv[1], filter(lambda kv: kv[0] and kv[1], zip(modifiers_keys, modifier_values))))
        # create modifier string
        modifier_string = '[' + ': '.join(modifier_list) + ']' if modifier_list else ''
        # return source, target
        return source, target + modifier_string

    def _replace_header_line(self, source_and_target_column_list, source_df):
        # create mapping from source to target columns, excluding empty source columns
        source_to_target_mapping = {k: v for k, v in source_and_target_column_list if k}
        # create list of additional columns with empty source column to append to the end
        additional_columns = [v for k, v in source_and_target_column_list if not k]
        # replace headers with target headers
        source_df.columns = [source_to_target_mapping.get(c, '') for c in source_df.columns]
        # append additional columns to df
        source_df = pd.concat([source_df, pd.DataFrame(columns=additional_columns)], axis=1)
        return source_df
