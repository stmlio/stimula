"""
This class returns the dtypes and converters for columns that require special handling.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import json
import re

import pandas as pd


class TypesCompiler:


    def compile(self, mapping, column_names):
        # only return enabled columns
        if 'columns' not in mapping:
            return {}

        # process all columns to obtain the converters, include empty columns
        columns = [self._column(c) for c in mapping['columns']]

        # create a dictionary of converters to read from csv
        read_csv_converters = {column_names[i]: column['read_csv_converter'] for i, column in enumerate(columns) if 'read_csv_converter' in column}

        # create a dictionary of converters to write to csv
        write_csv_converters = {column_names[i]: column['write_csv_converter'] for i, column in enumerate(columns) if 'write_csv_converter' in column}

        # create a dictionary of converters to read from db
        read_db_converters = {column_names[i]: column['read_db_converter'] for i, column in enumerate(columns) if 'read_db_converter' in column}

        # create a dictionary of dtypes for read_csv
        read_csv_dtypes = {column_names[i]: column['read_csv_dtype'] for i, column in enumerate(columns) if 'read_csv_dtype' in column}

        # get the columns that must be parsed as dates
        read_csv_parse_dates = [column_names[i] for i, column in enumerate(columns) if column.get('read_csv_parse_dates', False)]

        # return the dictionary of converters and dtypes
        result = {
            'read_csv_converters': read_csv_converters,
            'write_csv_converters': write_csv_converters,
            'read_db_converters': read_db_converters,
            'read_csv_dtypes': read_csv_dtypes,
            'read_csv_parse_dates': read_csv_parse_dates}

        return result

    def _column(self, column):
        # empty column does not have attributes
        if 'attributes' not in column:
            return {}
        attributes = self._attributes(column['attributes'])

        result = {}

        # to read json from csv, we need to convert the string to json
        if len(attributes) == 1 and attributes[0]['type'] == 'jsonb':
            result['read_csv_converter'] = json_to_dict
            result['write_csv_converter'] = dict_to_json

        # to read binary string from csv, we need to convert the string to binary
        if len(attributes) == 1 and attributes[0]['type'] == 'bytea':
            result['read_csv_converter'] = binary_string_converter

        # to read binary string from db, we need to convert the binary to string
        if len(attributes) == 1 and attributes[0]['type'] == 'bytea':
            result['read_db_converter'] = memoryview_to_string_converter

        # set the dtype for read_csv
        result['read_csv_dtype'] = self._dtype(attributes)

        # set whether to parse the column as a date
        if self._date_type(attributes):
            result['read_csv_parse_dates'] = True

        # convert date to pandas datetime when reading from db
        if len(attributes) == 1 and attributes[0]['type'] == 'date':
            result['read_db_converter'] = date_to_datetime_converter


        return result

    def _attributes(self, attributes):
        # iterate attributes to get list of lists
        attributes_lists = [self._attribute(a) for a in attributes]
        # flatten list of lists
        attributes = [item for sublist in attributes_lists for item in sublist]
        return attributes

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            # return attribute types from foreign key
            return self._attributes(foreign_key['attributes'])

        return [attribute]

    def _dtype(self, attributes):
        # if there are multiple types, return 'string'
        if len(attributes) > 1:
            return 'string'

        # return attribute type as list
        type = attributes[0].get('type')

        # read boolean as boolean
        if type == 'boolean':
            return 'boolean'

        # read integer as Int64
        if type == 'integer':
            return 'Int64'

        # read numeric as Float64
        if type == 'numeric':
            return 'float'

        # replace timestamp and date columns with 'object', because pandas doesn't like datetime64 columns when reading from csv
        if type == 'timestamp' or type == 'date':
            return 'object'

        # read jsonb as object
        if type == 'jsonb':
            return 'object'

        # double precision as float
        if type == 'double precision':
            return 'float'

        # binary string as object
        if type == 'bytea':
            return 'object'

        # default to string
        return 'string'

    def _date_type(self, attributes):
        # return True if there's a single attribute with type 'date' or 'timestamp'
        if len(attributes) == 0:
            return False
        attribute_type = attributes[0].get('type')
        return attribute_type == 'date' or attribute_type == 'timestamp'


def json_to_dict(json_str):
    # accept json strings that come from CSV using single quotes.

    # accept empty string, return None
    if not json_str:
        return None

    # get index of first double quote
    first_double_quote = json_str.find('"')

    # get index of first single quote
    first_single_quote = json_str.find("'")

    # if there are no double quotes, or if there are double quotes but there's a single quote that comes before the first double quote
    if first_double_quote == -1 or (0 <= first_single_quote < first_double_quote):
        # escape double quotes
        json_str = json_str.replace('"', '\\"')
        json_str = json_str.replace('\n', '\\n')
        # and replace all single quotes with double quotes
        json_str = json_str.replace("'", '"')

    # parse json string
    try:
        # convert json string into dictionary
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error parsing JSON string: {json_str}", e.doc, e.pos) from None


def dict_to_json(dict):
    return json.dumps(dict, ensure_ascii=False)


# Custom converter function to handle binary string data
def binary_string_converter(mixed_string):
    # handle empty string as None, because that's how we probably mean to store it in the DB
    if not mixed_string:
        return None

    # Define a regular expression pattern to match the escaped sequences
    escaped_pattern = re.compile(r'(\\x[0-9a-fA-F]{2})+')

    # Define a function to decode the escaped sequences
    def decode_escaped(match):
        # Extract the hexadecimal characters and decode them using UTF-8
        hex = match.group(0).replace('\\x', '')
        try:
            # try to decode as utf-8
            return bytes.fromhex(hex).decode('utf-8')
        except:
            try:
                # if it fails, try to decode as latin-1
                return bytes.fromhex(hex).decode('latin-1')
            except:
                # if it fails, return the hex string
                return match.group(0)

    # Replace the escaped sequences with their decoded counterparts
    decoded_string = escaped_pattern.sub(decode_escaped, mixed_string)

    return decoded_string


def memoryview_to_string_converter(mv):
    # return None for empty values
    if mv is None:
        return None

    # Function to convert memoryview objects to strings
    try:
        # try to decode as utf-8
        return mv.tobytes().decode('utf-8')
    except UnicodeDecodeError:
        try:
            # try to decode as latin-1
            return mv.tobytes().decode('latin-1')
        except UnicodeDecodeError:
            # if all fails, return as binary
            return mv.tobytes()

def date_to_datetime_converter(date):
    # return None for empty values
    if date is None:
        return None

    # convert date from database to pandas datetime, so that we can accurately compare it to a date read from CSV
    return pd.to_datetime(date)