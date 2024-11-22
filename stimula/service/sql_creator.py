"""
This script provides the concrete ExecutorCreator implementation to insert, update and delete using SQL.

Author: Romke Jonker
Email: romke@stml.io
"""

from .executor_creator import ExecutorCreator
from .query_executor import SimpleQueryExecutor, DependentQueryExecutor, OperationType
from ..stml.header_renderer import HeaderRenderer
from ..stml.model import Entity, Reference, Attribute
from ..stml.sql.delete_renderer import DeleteRenderer
from ..stml.sql.insert_renderer import InsertRenderer, ReturningClauseRenderer
from ..stml.sql.update_renderer import UpdateRenderer


class InsertSqlCreator(ExecutorCreator):
    def __init__(self):
        super().__init__()
        self.operation_type = OperationType.INSERT

    def _create_executor(self, line_number, mapping: Entity, values, context, orm):
        # Compile the filtered tree to get the query.
        query = InsertRenderer().render(mapping)

        # is there an extension on the root table?
        if not ReturningClauseRenderer().render(mapping):
            # yield query and split columns
            return SimpleQueryExecutor(line_number, self.operation_type, mapping.name, query, values, context)

        else:

            # create a separate query for the extension
            dependent_query = self._create_extension_insert_query(mapping, values)

            # return dependent query executor to execute both queries
            return DependentQueryExecutor(line_number, self.operation_type, mapping.name, context, (query, values), dependent_query)

    def _create_non_unique_value_dict(self, mapping, row):
        # call super to get all other columns and values
        non_unique_value_dict = super()._create_non_unique_value_dict(mapping, row)

        # remove null and nan values, because we don't need to insert them
        non_unique_value_dict = {key: value for key, value in non_unique_value_dict.items() if not self._is_empty(value)}

        return non_unique_value_dict

    def _create_extension_insert_query(self, mapping, param):
        # get table, name parameter name and values
        table, name_parameter_name, values = ExtensionValueHelper().get_extension_parameter_values(mapping, param)

        # create insert query
        sql = f'insert into {table} (name, module, model, res_id) values (:{name_parameter_name}, :module, :model, :res_id)'
        return sql, values


class UpdateSqlCreator(ExecutorCreator):

    def __init__(self):
        super().__init__()
        self.operation_type = OperationType.UPDATE

    def _create_executor(self, line_number, mapping, values, context, orm):
        # Compile the filtered tree to get the query.
        query = UpdateRenderer().render(mapping)

        # yield query and split columns
        return SimpleQueryExecutor(line_number, self.operation_type, mapping.name, query, values, context)

    def _create_unique_value_dict(self, mapping, row):
        # split row in self and other
        self_row, other_row = self._split_diff_self_other(row)

        # get unique headers
        unique_headers = HeaderRenderer().render_list_unique(mapping)

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
        unique_headers = HeaderRenderer().render_list_unique(mapping)

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
        self_row = row[[column for column in row.index if (column[0] != '__line__' and column[1] in ['', 'self'])]]
        other_row = row[[column for column in row.index if (column[0] != '__line__' and column[1] in ['', 'other'])]]

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


class DeleteSqlCreator(ExecutorCreator):
    def __init__(self):
        super().__init__()
        self.operation_type = OperationType.DELETE

    def _create_executor(self, line_number, mapping: Entity, values, context, orm):
        # Compile the filtered tree to get the query.
        query = DeleteRenderer().render(mapping)

        # is there an extension on the root table?
        if not ReturningClauseRenderer().render(mapping):
            # yield query and split columns
            return SimpleQueryExecutor(line_number, self.operation_type, mapping.name, query, values, context)

        else:

            # create a separate query for the extension
            dependent_query = self._create_extension_delete_query(mapping, values)

            # return dependent query executor to execute both queries
            return DependentQueryExecutor(line_number, self.operation_type, mapping.name, context, (query, values), dependent_query)

    def _create_non_unique_value_dict(self, mapping, row):
        # For deletes we don't need non-unique columns.
        # Except, we do need extension name value, because we need to delete the extension even if it's not unique
        return super()._create_non_unique_root_extension_value_dict(mapping, row)

    def _get_line_number(self, row):
        # deletes don't have a matching input row
        return None

    def _create_extension_delete_query(self, mapping, param):
        # get table, name parameter name and values
        table, name_parameter_name, values = ExtensionValueHelper().get_extension_parameter_values(mapping, param)

        # create delete query
        sql = f'delete from {table} where name = :{name_parameter_name} and module = :module and model = :model and res_id = :res_id'
        return sql, values


class ExtensionValueHelper:
    def get_extension_parameter_values(self, mapping: Entity, param):
        # get extension foreign keys
        extension_foreign_keys = [a for a in mapping.attributes if isinstance(a, Reference) and a.extension]

        # only one extension is supported for now
        assert len(extension_foreign_keys) == 1, f'Only one extension is supported, found: {len(extension_foreign_keys)}'
        foreign_key = extension_foreign_keys[0]

        # foreign key must have a single attribute
        assert len(foreign_key.attributes) == 1, f'Foreign key must have a single attribute, found: {len(foreign_key["attributes"])}'
        attribute = foreign_key.attributes[0]
        assert isinstance(attribute, Attribute), f'Foreign key attribute must be an attribute, found: {type(attribute)}'
        name_parameter_name = attribute.parameter

        table = foreign_key.table
        name = param[name_parameter_name]
        qualifier = foreign_key.qualifier
        # TODO: replace with proper way to get the model name
        model = mapping.name.replace('_', '.')

        # create values map
        values = {name_parameter_name: name, 'module': qualifier, 'model': model, 'res_id': None}

        # return table, name parameter name and values
        return table, name_parameter_name, values
