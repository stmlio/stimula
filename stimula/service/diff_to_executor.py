"""
This class takes a diff and creates query executors for each diff type.
It also makes the split between using SQL, ORM or any other way to execute the queries.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from typing import Optional

from .abstract_orm import AbstractORM
from .orm_creator import InsertOrmCreator, UpdateOrmCreator, DeleteOrmCreator
from .sql_creator import InsertSqlCreator, UpdateSqlCreator, DeleteSqlCreator
from ..stml.alias_enricher import AliasEnricher


class DiffToExecutor:

    def __init__(self):
        pass

    def diff_executor(self, mapping, diffs, context=None, orm: Optional[AbstractORM] = None):

        if not self._use_orm(mapping):
            # create SQL query executors
            return self._sql_executor(mapping, diffs, context)
        else:
            # assert that orm exists
            assert orm is not None, 'ORM is required for this mapping'

            # create ORM executors
            return self._orm_executor(mapping, diffs, context, orm)

    def _use_orm(self, mapping):
        # hard coded for now
        return mapping.name in ['ir_attachment']

    def _sql_executor(self, mapping, diffs, context=None):
        # get from tuple
        inserts, updates, deletes = diffs

        # add alias and parameter names to mapping before creating sql
        aliased_mapping = AliasEnricher().enrich(mapping)

        # create sql for each diff
        insert_sql = list(InsertSqlCreator().create_executors(aliased_mapping, inserts, context))
        update_sql = list(UpdateSqlCreator().create_executors(aliased_mapping, updates, context))
        delete_sql = list(DeleteSqlCreator().create_executors(aliased_mapping, deletes, context))

        return insert_sql + update_sql + delete_sql

    def _orm_executor(self, mapping, diffs, context, orm):

        # get from tuple
        inserts, updates, deletes = diffs

        # add alias and parameter names to mapping before creating sql
        aliased_mapping = AliasEnricher().enrich(mapping)

        # create sql for each diff
        insert_orm = list(InsertOrmCreator().create_executors(aliased_mapping, inserts, context, orm))
        update_orm = list(UpdateOrmCreator().create_executors(aliased_mapping, updates, context, orm))
        delete_orm = list(DeleteOrmCreator().create_executors(aliased_mapping, deletes, context, orm))

        return insert_orm + update_orm + delete_orm
