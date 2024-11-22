"""
This script provides the abstract SqlCreator and concrete classes for Insert, Update, and Delete SQL creators.

Turning a diff into SQL statements works as follows:

1. Iterate rows in diff, because each query depends on the values in a row. Then repeat the following steps for each row.
2. Create a dictionary with unique column headers as keys and values as values. We'll need these for all query types.
3. Create a dictionary with other column headers as keys and values as values. Include non-null values for inserts and modified values for updates. Leave it empty for deletes.
4. Filter tree based on keys in the two dictionaries.
5. Compile the filtered tree to get parameter names for the query.
6. For each column in the tree, look up values by header using the dictionaries created earlier
7. Split values if the column has more than one parameter names. A CSV cell can contain multiple values separated by a colon
7. Clean up values. Strip whitespace from strings. Convert values to match the DB schema. For example, convert '1' to 1 if the column is an int
8. Create the dictionary with parameter names and values.
9. Compile the filtered tree to get the query for a row. Use the parameter dictionary where needed. For example, in SQL you write 'x = 1' but 'x is null'
10. Return query and parameter dictionary

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from abc import ABC, abstractmethod

import numpy
import pandas as pd
from psycopg2._json import Json

from .query_executor import FailedQueryExecutor
from ..stml.header_renderer import HeaderRenderer
from ..stml.model import Entity
from ..stml.sql.parameter_types_renderer import ParameterTypesRenderer
from ..stml.sql.parameters_renderer import ParametersRenderer
from ..stml.values_parser import ValuesLexer, ValuesParser


class ExecutorCreator(ABC):

    def __init__(self):
        self._values_lexer = ValuesLexer()
        self._values_parser = ValuesParser()
        self.operation_type = None

    def create_executors(self, mapping, diffs, context=None, orm=None):

        # iterate rows in diff
        for i in range(len(diffs)):
            row = diffs.iloc[i]

            # return line number, depends on operation type
            line_number = self._get_line_number(row)

            try:
                # create and yield executor
                yield self._prepare_and_create_executor(mapping, row, line_number, context, orm)

            except Exception as e:

                # yield query with line number and error message
                yield FailedQueryExecutor(line_number, self.operation_type, mapping.name, context, str(e))

    def _prepare_and_create_executor(self, mapping, row, line_number, context=None, orm=None):
        # prepare mapping and values for row
        filtered_mapping, value_dict = self._prepare_mapping_and_values(mapping, row)

        # create and return executor
        return self._create_executor(line_number, filtered_mapping, value_dict, context, orm)

    def _prepare_mapping_and_values(self, mapping, row):
        # Create a dictionary with unique column headers as keys and values as values. We'll need these for all query types.
        unique_value_dict = self._create_unique_value_dict(mapping, row)

        # raise error if there are no unique values
        assert unique_value_dict, 'Header must have at least one unique column'

        # Create a dictionary with other column headers as keys and values as values.
        non_unique_value_dict = self._create_non_unique_value_dict(mapping, row)

        # create combined dictionary for convenience
        header_value_dict = {**unique_value_dict, **non_unique_value_dict}

        # Filter tree based on keys in the combined dictionary
        filtered_mapping = self._filter_mapping(mapping, header_value_dict)

        # Compile the filtered tree to get parameter names for the query.
        parameter_names = ParametersRenderer().render(filtered_mapping)

        # Create the dictionary with parameter names as keys and values as values. An element may contain multiple parameters and values
        parameter_value_dict = self._map_parameter_names_with_values(filtered_mapping, parameter_names, header_value_dict)

        # Split values if the column has more than one parameter names. A CSV cell can contain multiple values separated by a colon
        split_parameter_value_dict = self._split_columns(parameter_names, parameter_value_dict)

        # get parameter types
        parameter_types = ParameterTypesRenderer().render(filtered_mapping)

        # Clean up values. Strip whitespace from strings. Convert values to match the DB schema. For example, convert '1' to 1 if the column is an int
        value_dict_clean = self._clean_values_in_dict(split_parameter_value_dict, parameter_types)

        return filtered_mapping, value_dict_clean

    def _create_unique_value_dict(self, mapping, row):
        # get unique column headers
        unique_headers = HeaderRenderer().render_list_unique(mapping)

        # create dictionary with unique column headers as keys and values as values
        unique_value_dict = {header: row[header] for header in unique_headers}

        return unique_value_dict

    def _create_non_unique_value_dict(self, mapping, row):
        # get non-unique headers
        non_unique_headers = HeaderRenderer().render_list_non_unique(mapping)

        # create dictionary with non-unique column headers as keys and values as values
        non_unique_value_dict = {header: row[header] for header in non_unique_headers}

        return non_unique_value_dict

    def _create_non_unique_root_extension_value_dict(self, mapping, row):
        # when we delete a record with an extension on the root table (not on a foreign key table),
        # then we need to delete the extension record as well. If the extension is unique, then
        # the delete statement will already have the extension's name value, but if it's not unique,
        # then this method ensures the value is included.

        # get non-unique headers
        non_unique_headers = HeaderRenderer().render_list_non_unique_root_extension(mapping)

        # create dictionary with non-unique column headers as keys and values as values
        non_unique_value_dict = {header: row[header] for header in non_unique_headers}

        return non_unique_value_dict

    def _filter_mapping(self, mapping: Entity, value_dict):
        # filter columns by those that have a value in value_dict

        # create a list of column headers
        headers = HeaderRenderer().render_list(mapping, include_skip=True, include_orm_only=True)

        # create a list of all columns
        all_columns = mapping.attributes

        assert len(headers) == len(all_columns), f'Number of headers must equal number of columns, found: {len(headers)} and {len(all_columns)}'

        # create a list of items in mapping where the corresponding header is in value_dict
        filter_columns = [item for item, header in zip(all_columns, headers) if header in value_dict]

        # create a copy of mapping with columns replaced by filtered columns
        filtered_mapping = Entity(mapping.name)
        filtered_mapping.attributes = filter_columns

        return filtered_mapping

    def _map_parameter_names_with_values(self, filtered_mapping, parameter_names, value_dict):
        # create a list of column headers. Include the ORM-only columns, because we need them to write to ORM.
        headers = HeaderRenderer().render_list(filtered_mapping, include_orm_only=True)

        # create a dictionary with parameter names as keys and values as values
        return {parameter_name: value_dict[header] for parameter_name, header in zip(parameter_names, headers)}

    def _split_columns(self, parameter_names, value_dict):
        # verify that parameter_names and value_dict have the same number of items
        assert len(parameter_names) == len(value_dict), f'Number of parameter names must equal number of values, found: {parameter_names} and {value_dict}'

        # create empty dictionary
        value_dict_split = {}

        # iterate parameter names
        for parameter_name in parameter_names:
            # split the value and add it to the dictionary
            value_dict_split.update(self._split_value(parameter_name, value_dict[parameter_name]))

        return value_dict_split

    def _split_value(self, names, values):
        # if there is only one name, there is only one value
        if len(names) == 1:
            return {names[0]: values}

        # else split values at colon, but make sure not to split at json colons
        split_values = self._values_parser.parse(self._values_lexer.tokenize(values))

        # verify that number of names equals number of values
        assert len(names) == len(split_values), f'Number of names must equal number of values, found: {names} and {split_values}'

        # create dictionary with parameter names as keys and values as values
        return {name: value for name, value in zip(names, split_values)}

    def _clean_values_in_dict(self, value_dict, parameter_types):
        return {key: self._clean_value_in_dict(value, parameter_types[key]) for key, value in value_dict.items()}

    def _clean_value_in_dict(self, value, type):
        # strip whitespace from strings only
        if isinstance(value, str):
            value = value.strip()

        # convert numpy.int64 to int
        if isinstance(value, numpy.int64):
            value = int(value)

        # convert numpy.float64 to float
        if isinstance(value, numpy.float64):
            value = float(value)

        # convert numpy bool to python bool
        if isinstance(value, numpy.bool_):
            # psycopg wants string representations for bools
            value = 'true' if value else 'false'

        # convert dict to json string
        if isinstance(value, dict):
            # use psycopg's Json to correctly convert unicode characters
            value = Json(value)

        # dict is stored in data frame as frozenset, bec/ the index requires hashable objects
        if isinstance(value, frozenset):
            # convert to dict, then use psycopg's Json to correctly convert unicode characters
            value = Json(dict(value))

        # type conversions
        if type == 'int':
            value = int(value)
        elif type in ['varchar', 'text']:
            value = str(value)
        elif type == 'float':
            value = float(value)
        elif type == 'bool':
            # psycopg wants string representations for bools
            value = 'true' if value else 'false'

        return value

    def _is_empty(self, value):
        return value is None or pd.isnull(value) or pd.isna(value) or value == ''

    @abstractmethod
    def _create_executor(self, line_number, mapping, value_dict, context, orm):
        raise NotImplementedError

    def _get_line_number(self, row):
        # for inserts and updates, just return __line__ column
        return row['__line__']
