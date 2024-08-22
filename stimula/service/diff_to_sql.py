"""
This class takes a diff and creates SQL statements for each diff type.

Author: Romke Jonker
Email: romke@rnadesign.net
"""

from .sql_creator import InsertSqlCreator, UpdateSqlCreator, DeleteSqlCreator
from ..compiler.alias_compiler import AliasCompiler


class DiffToSql:

    def __init__(self):
        self._insert_sql_creator = InsertSqlCreator()
        self._update_sql_creator = UpdateSqlCreator()
        self._delete_sql_creator = DeleteSqlCreator()

    def diff_sql(self, mapping, diffs, context=None):
        # get from tuple
        inserts, updates, deletes = diffs

        # add alias and parameter names to mapping before creating sql
        aliased_mapping = AliasCompiler().compile(mapping)

        # create sql for each diff
        insert_sql = list(self._insert_sql_creator.create_sql(aliased_mapping, inserts, context))
        update_sql = list(self._update_sql_creator.create_sql(aliased_mapping, updates, context))
        delete_sql = list(self._delete_sql_creator.create_sql(aliased_mapping, deletes, context))

        return insert_sql + update_sql + delete_sql
