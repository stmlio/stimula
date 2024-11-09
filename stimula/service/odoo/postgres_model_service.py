"""
This class provides access to the database model in a Postgres database.

Author: Romke Jonker
Email: romke@stml.io
"""

from stimula.service.model_service import ModelService


class PostgresModelService(ModelService):
    def __init__(self, meta):
        self._meta = meta

    def get_table(self, table_name):
        table = self._meta.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found")
        return table

    def find_primary_keys(self, table):
        # return names of primary keys in table
        return [column.name for column in table.primary_key]

    def resolve_foreign_key_table(self, table, column_name):
        # get referred column
        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table.name}'")
        column = table.columns[column_name]
        # only know how to deal with a single foreign key per column
        if len(column.foreign_keys) != 1:
            # this is either an error condition or an extension relation, with the foreign key in the extension table. We can tell once we have parsed the modifiers.
            return None, column_name

        foreign_key = list(column.foreign_keys)[0]
        foreign_table = foreign_key.column.table
        foreign_column_name = foreign_key.column.name
        return foreign_table, foreign_column_name