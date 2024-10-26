'''
this class is responsible for executing ORM queries

Author: Romke Jonker
Email: romke@stml.io
'''
from abc import abstractmethod

import pandas as pd

from stimula.service.abstract_orm import AbstractORM
from stimula.service.query_executor import Executor, ExecutionResult


class OrmExecutor(Executor):
    def __init__(self, line_number, operation_type, table, query, query_values, orm_values, context, orm):
        super().__init__(line_number, operation_type, table, context)
        self._query: str = query
        self._query_values: dict = query_values
        self._orm_values: dict = orm_values
        self._orm: AbstractORM = orm

    def execute(self, cursor):

        key_values = {}

        if self._query:
            try:
                # execute query
                key_values = self._execute_select_query(cursor, self._query, self._query_values)
            except Exception as e:
                error = str(e)
                return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self._query, self._query_values, self.context, error=error)


        # combine orm values and key values into a new dictionary
        params = {**self._orm_values, **key_values}

        # execute ORM method
        return self._execute_orm_method(self.table_name, params)

    def _execute_select_query(self, cursor, query: str, params: dict):
        # replace ':' style place holders with '%' style
        psycopg_query = self._replace_placeholders(self._query)
        # replace NA values with None in params dictionary
        params_with_none = {k: None if pd.isna(v) else v for k, v in self._query_values.items()}
        cursor.execute(psycopg_query, params_with_none)
        row = cursor.fetchone()
        rowcount = cursor.rowcount

        # verify row was affected
        if rowcount == 0:
            raise ValueError("No row was affected. Query: %s, Params: %s" % (query, params))

        # verify no more than one row was affected
        if rowcount > 1:
            # raise exception, bec/ we must not commit the transaction
            raise ValueError("More than one row was affected. Inserts: %s, Query: %s, Params: %s" % (rowcount, query, params))

        column_names = [desc[0] for desc in cursor.description]

        # Combine the column names with the row values into a dictionary
        row_dict = dict(zip(column_names, row))

        return row_dict

    @abstractmethod
    def _execute_orm_method(self, model_name: str, params: dict):
        raise NotImplementedError

    def fake_execute(self):
        pass

    def queries(self):
        pass


class CreateOrmExecutor(OrmExecutor):
    def _execute_orm_method(self, model_name: str, values: dict):
        new_record = self._orm.create(model_name, values)
        if not new_record:
            return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self._query, self._query_values, self.context)

        return ExecutionResult(self.line_number, self.operation_type, True, 1, self.table_name, self._query, self._query_values, self.context, new_record)


class UpdateOrmExecutor(OrmExecutor):
    def _execute_orm_method(self, model_name: str, record_id: int, values: dict):
        self._orm.update(model_name, record_id, values)


class DeleteOrmExecutor(OrmExecutor):
    def _execute_orm_method(self, model_name: str, record_id: int, values: dict):
        self._orm.delete(model_name, record_id)
