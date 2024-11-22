"""
This class provides access to the database model in a Postgres database.

Author: Romke Jonker
Email: romke@stml.io
"""
import pandas as pd
from sqlalchemy import select, func

from stimula.service.context import cnx_context, get_metadata
from stimula.service.model_service import ModelService
from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.select_renderer import SelectRenderer


class PostgresModelService(ModelService):
    def __init__(self):
        self._meta = None

    @property
    def meta(self):
        if self._meta is None:
            self._meta = get_metadata(cnx_context.cnx)
        return self._meta

    def get_table(self, table_name):
        table = self.meta.tables.get(table_name)
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

    def get_non_empty_columns(self, table):
        # create list of column names
        column_names = [c.name for c in table.columns]
        # create column expressions
        expr = select(*[func.bool_or(column.isnot(None)) for column in table.columns])
        # execute query
        cr = cnx_context.cr
        cr.execute(str(expr))
        result = cr.fetchone()
        # zip and filter non-null columns
        result = [c[0] for c in zip(column_names, result) if c[1]]
        # return list
        return result

    def read_table(self, mapping: dict, where_clause=None):
        # get sqlalchemy engine from context
        engine = cnx_context.engine

        # create select query
        query = self._create_select_query(mapping, where_clause)

        # read dataframe from DB
        return pd.read_sql_query(query, engine)

    def _create_select_query(self, mapping, where_clause):
        # add aliases and parameter names
        aliased_mapping = AliasEnricher().enrich(mapping)

        # translate syntax tree to select query
        return SelectRenderer().render(aliased_mapping, where_clause)
