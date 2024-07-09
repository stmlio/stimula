"""
This class takes a diff and creates SQL statements for each diff type.

Author: Romke Jonker
Email: romke@rnadesign.net
"""

from .sql_creator import InsertSqlCreator, UpdateSqlCreator, DeleteSqlCreator


class DiffToSql:

    def __init__(self):
        self._insert_sql_creator = InsertSqlCreator()
        self._update_sql_creator = UpdateSqlCreator()
        self._delete_sql_creator = DeleteSqlCreator()

    def diff_sql(self, mapping, diffs):
        # get from tuple
        inserts, updates, deletes = diffs

        # create sql for each diff
        insert_sql = list(self._insert_sql_creator.create_sql(mapping, inserts))
        update_sql = list(self._update_sql_creator.create_sql(mapping, updates))
        delete_sql = list(self._delete_sql_creator.create_sql(mapping, deletes))

        return insert_sql, update_sql, delete_sql
