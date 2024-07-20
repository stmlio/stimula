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
import json

import numpy
import pandas as pd
from numpy import int64
from psycopg2._json import Json

from .query_executor import SimpleQueryExecutor, DependentQueryExecutor
from ..compiler.alias_compiler import AliasCompiler
from ..compiler.delete_compiler import DeleteCompiler
from ..compiler.header_compiler import HeaderCompiler
from ..compiler.insert_compiler import InsertCompiler, ReturningClauseCompiler
from ..compiler.parameters_compiler import ParametersCompiler
from ..compiler.update_compiler import UpdateCompiler
from ..header.values_parser import ValuesLexer, ValuesParser


class SqlCreator:

    def __init__(self):
        self._values_lexer = ValuesLexer()
        self._values_parser = ValuesParser()

    def create_sql(self, mapping, diffs):
        # add alias and parameter names to mapping
        aliased_mapping = AliasCompiler().compile(mapping)

        # iterate rows in diff
        for i in range(len(diffs)):
            row = diffs.iloc[i]

            # create query for row
            query, value_dict = self._create_sql_row(aliased_mapping, row)

            # yield query and split columns
            yield SimpleQueryExecutor(query, value_dict)

    def _create_sql_row(self, mapping, row):
        # Create a dictionary with unique column headers as keys and values as values. We'll need these for all query types.
        unique_value_dict = self._create_unique_value_dict(mapping, row)

        # Create a dictionary with other column headers as keys and values as values.
        non_unique_value_dict = self._create_non_unique_value_dict(mapping, row)

        # create combined dictionary for convenience
        header_value_dict = {**unique_value_dict, **non_unique_value_dict}

        # Filter tree based on keys in the combined dictionary
        filtered_mapping = self._filter_mapping(mapping, header_value_dict)

        # Compile the filtered tree to get parameter names for the query.
        parameter_names = ParametersCompiler().compile(filtered_mapping)

        # Create the dictionary with parameter names as keys and values as values. An element may contain multiple parameters and values
        parameter_value_dict = self._map_parameter_names_with_values(filtered_mapping, parameter_names, header_value_dict)

        # Split values if the column has more than one parameter names. A CSV cell can contain multiple values separated by a colon
        split_parameter_value_dict = self._split_columns(parameter_names, parameter_value_dict)

        # Clean up values. Strip whitespace from strings. Convert values to match the DB schema. For example, convert '1' to 1 if the column is an int
        value_dict_clean = self._clean_values_in_dict(split_parameter_value_dict)

        # Compile the filtered tree to get the query for a row. Use the parameter dictionary where needed. For example, in SQL you write 'x = 1' but 'x is null'
        query = self._create_query(filtered_mapping, value_dict_clean)

        return query, value_dict_clean

    def _create_unique_value_dict(self, mapping, row):
        # get unique column headers
        unique_headers = HeaderCompiler().compile_list_unique(mapping)

        # create dictionary with unique column headers as keys and values as values
        unique_value_dict = {header: row[header] for header in unique_headers}

        return unique_value_dict

    def _create_non_unique_value_dict(self, mapping, row):
        # get non-unique headers
        non_unique_headers = HeaderCompiler().compile_list_non_unique(mapping)

        # create dictionary with non-unique column headers as keys and values as values
        non_unique_value_dict = {header: row[header] for header in non_unique_headers}

        return non_unique_value_dict

    def _filter_mapping(self, mapping, value_dict):
        # filter columns by those that have a value in value_dict

        # create a list of column headers
        headers = HeaderCompiler().compile_list(mapping, include_skip=True)

        # create a list of all columns
        all_columns = mapping['columns']

        assert len(headers) == len(all_columns), f'Number of headers must equal number of columns, found: {len(headers)} and {len(all_columns)}'

        # create a list of items in mapping where the corresponding header is in value_dict
        filter_columns = [item for item, header in zip(all_columns, headers) if header in value_dict]

        # create a copy of mapping with columns replaced by filtered columns
        filtered_mapping = mapping.copy()
        filtered_mapping['columns'] = filter_columns

        return filtered_mapping

    def _map_parameter_names_with_values(self, filtered_mapping, parameter_names, value_dict):
        # create a list of column headers
        headers = HeaderCompiler().compile_list(filtered_mapping)

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

    def _clean_values_in_dict(self, value_dict):
        return {key: self._clean_value_in_dict(value) for key, value in value_dict.items()}

    def _clean_value_in_dict(self, value):
        # strip whitespace from strings only
        if isinstance(value, str):
            value = value.strip()

        # convert int64 to int
        if isinstance(value, int64):
            value = int(value)

        # convert numpy bool to python bool
        if isinstance(value, numpy.bool_):
            # psycopg wants string representations for bools
            value = 'true' if value else 'false'

        # convert dict to json string
        if isinstance(value, dict):
            # use psycopg's Json to correctly convert unicode characters
            value = Json(value)

        return value

    def _create_query(self, tree, value_dict):
        # raise exception because this method must be overridden
        raise NotImplementedError


class InsertSqlCreator(SqlCreator):

    def create_sql(self, mapping, diffs):
        for query_executor in super().create_sql(mapping, diffs):

            # is there an extension on the root table?
            if not ReturningClauseCompiler().compile(mapping):
                yield query_executor

            else:

                # create a separate query for the extension
                dependent_query = self._create_extension_insert_query(mapping, query_executor.params)
                dependent_query_executor = DependentQueryExecutor((query_executor.query, query_executor.params), dependent_query)
                yield dependent_query_executor


    def _create_non_unique_value_dict(self, mapping, row):
        # call super to get all other columns and values
        non_unique_value_dict = super()._create_non_unique_value_dict(mapping, row)

        # remove null and nan values
        non_unique_value_dict = {key: value for key, value in non_unique_value_dict.items() if self._has_value(value)}

        return non_unique_value_dict

    def _has_value(self, value):
        return value is not None and not pd.isnull(value) and not pd.isna(value) and not value == ''

    def _create_query(self, mapping, value_dict):
        # create insert query
        return InsertCompiler().compile(mapping)

    def _create_extension_insert_query(self, mapping, param):
        # get extension foreign keys
        extension_foreign_keys = [a['foreign-key'] for c in mapping['columns'] for a in c.get('attributes', []) if a.get('foreign-key', {}).get('extension')]

        # only one extension is supported for now
        assert len(extension_foreign_keys) == 1, f'Only one extension is supported, found: {len(extension_foreign_keys)}'

        foreign_key = extension_foreign_keys[0]
        table = foreign_key['table']
        name = param['name']
        qualifier = foreign_key['qualifier']
        # TODO: replace with proper way to get the model name
        model = mapping['table'].replace('_', '.')
        # create query

        sql = f'insert into {table} (name, module, model, res_id) values (:name, :module, :model, :res_id)'
        values = {'name': name, 'module': qualifier, 'model': model, 'res_id': None}
        return sql, values


class UpdateSqlCreator(SqlCreator):
    def _create_unique_value_dict(self, mapping, row):
        # split row in self and other
        self_row, other_row = self._split_diff_self_other(row)

        # get unique headers
        unique_headers = HeaderCompiler().compile_list_unique(mapping)

        # create dictionary with unique column headers as keys and values as values
        self_unique_value_dict = {header: self_row[header] for header in unique_headers}
        other_unique_value_dict = {header: other_row[header] for header in unique_headers}

        # validate that values for self and other are the same, we can't change unique values
        assert self_unique_value_dict == other_unique_value_dict, f'Values for unique columns must be the same for self and other, found: {self_unique_value_dict} and {other_unique_value_dict}'

        return self_unique_value_dict

    def _create_non_unique_value_dict(self, mapping, row):
        # split row in self and other
        self_row, other_row = self._split_diff_self_other(row)

        # get unique columns
        unique_headers = HeaderCompiler().compile_list_unique(mapping)

        # get self columns by removing unique headers from self row keys
        non_unique_headers = [header for header in self_row.keys() if header not in unique_headers]

        # create new result dictionary
        non_unique_value_dict = {}

        # iterate non-unique self headers
        for header in non_unique_headers:

            # get self and other values
            self_value = self_row[header]
            other_value = other_row[header]

            # if self and other values are different, add them to the result dictionary
            if self._is_value_modified(self_value, other_value):
                # these values may be <NA>. We need to convert them to None before passing them to the DB, but
                # better to keep them here as <NA> bec/ pandas doesn't like None values. The constructor converts None to <NA>
                # in our test cases.
                non_unique_value_dict[header] = self_value

        # verify that there is at least one modified value
        assert len(non_unique_value_dict) > 0, f'At least one non-unique value must be modified, found: {len(non_unique_value_dict)}'

        return non_unique_value_dict

    def _split_diff_self_other(self, row):
        # extract self and other columns into a new Series
        self_row = row[[column for column in row.index if (column[1] in ['', 'self'])]]
        other_row = row[[column for column in row.index if (column[1] in ['', 'other'])]]

        # replace header tuples with original column names
        self_row.index = [column[0] for column in self_row.index]
        other_row.index = [column[0] for column in other_row.index]

        # return self and other rows
        return self_row, other_row

    def _is_value_modified(self, this, that):
        # if both values are null, then it's not modified
        if self._is_empty(this) and self._is_empty(that):
            return False

        # if one value is null and the other value is not null, it's modified
        if self._is_empty(this) != self._is_empty(that):
            return True

        # else compare non-null values
        return this != that

    def _is_empty(self, value):
        return value is None or pd.isnull(value) or pd.isna(value) or value == ''

    def _create_query(self, tree, value_dict):
        # create query
        return UpdateCompiler().compile(tree)


class DeleteSqlCreator(SqlCreator):
    def _create_non_unique_value_dict(self, mapping, row):
        # return empty dictionary, because for deletes we don't need other columns
        return {}

    def _create_query(self, mapping, value_dict):
        # create query
        return DeleteCompiler().compile(mapping)
