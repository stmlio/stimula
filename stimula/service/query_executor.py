import logging
import re
from abc import ABC, abstractmethod
from enum import Enum

import pandas as pd

'''
This class allows for different execution styles. 
In particular, it allows for dependent queries, where the result of the first query is used as a parameter in the second query.
'''

_logger = logging.getLogger(__name__)


class QueryExecutor(ABC):
    @abstractmethod
    def execute(self, cursor):
        pass

    @abstractmethod
    def queries(self):
        pass

    def _replace_placeholders(self, query):
        # replace :xyz with %(xyz)s using regex
        return re.sub(r':(\w+)', r'%(\1)s', query)


class SimpleQueryExecutor(QueryExecutor):
    def __init__(self, line_number, operation_type, table_name, query, params):
        self.line_number = line_number
        self.operation_type = operation_type
        self.table_name = table_name
        self.query = query
        self.params = params

    def queries(self):
        return [(self.query, self.params)]

    def execute(self, cursor):
        # replace ':' style place holders with '%' style
        psycopg_query = self._replace_placeholders(self.query)
        # replace NA values with None in params dictionary
        params_with_none = {k: None if pd.isna(v) else v for k, v in self.params.items()}

        try:
            # execute query
            cursor.execute(psycopg_query, params_with_none)
        except Exception as e:
            error = str(e)
            return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self.query, self.params, error)

        # Get the number of affected rows
        rowcount = cursor.rowcount

        # verify row was inserted
        if rowcount == 0:
            error = 'No row was inserted'
            return ExecutionResult(self.line_number, self.operation_type, False, rowcount, self.table_name, self.query, self.params, error)

        # verify no more than one row was inserted
        if rowcount > 1:
            # we must not commit the transaction
            error = "More than one row was inserted, do not commit."
            return ExecutionResult(self.line_number, self.operation_type, False, rowcount, self.table_name, self.query, self.params, error, True)

        result = ExecutionResult(self.line_number, self.operation_type, True, rowcount, self.table_name, self.query, self.params)
        return result


class DependentQueryExecutor(QueryExecutor):
    def __init__(self, initial_query, dependent_query):
        self.query = initial_query[0]
        self.params = initial_query[1]
        self.dependent_query = dependent_query

    def queries(self):
        return [(self.query, self.params), self.dependent_query]

    def execute(self, cursor):
        # replace ':' style place holders with '%' style
        query_0 = self._replace_placeholders(self.query)

        # replace NA values with None in params dictionary
        params_0 = {k: None if pd.isna(v) else v for k, v in self.params.items()}

        cursor.execute(query_0, params_0)
        result = cursor.fetchone()
        rowcount = cursor.rowcount

        # verify row was inserted
        if rowcount == 0:
            _logger.warning("No row was inserted. Query: %s, Params: %s" % (query_0, params_0))
            return (rowcount, query_0, params_0)

        # verify no more than one row was inserted
        if rowcount > 1:
            # raise exception, bec/ we must not commit the transaction
            raise ValueError("More than one row was inserted. Inserts: %s, Query: %s, Params: %s" % (rowcount, query_0, params_0))

        # replace ':' style place holders with '%' style
        query_1 = self._replace_placeholders(self.dependent_query[0])
        params_1 = self.dependent_query[1]
        params_1['res_id'] = result[0]

        cursor.execute(query_1, params_1)
        result = (rowcount, query_0, params_0)

        return result


class OperationType(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    def __str__(self):
        return self.value

    def __repr__(self):
        return "'" + self.value + "'"


class ExecutionResult:
    def __init__(self, line_number, operation_type, success, rowcount, table_name, query, params, error=None, block_commit=False):
        self.line_number = line_number
        if not isinstance(operation_type, OperationType):
            raise ValueError(f"operation_type must be an instance of OperationType Enum, not {type(operation_type)}")
        self.operation_type = operation_type
        self.success = success
        self.rowcount = rowcount
        self.table = table_name
        self.query = query
        self.params = params
        self.error = error
        self.block_commit = block_commit

    def __str__(self):
        return f"Type: {self.operation_type}, Success: {self.success}, Rowcount: {self.rowcount}, Table: {self.table_name}, Query: {self.query}, Params: {self.params}"

    def to_dict(instance, execute):
        if execute:
            return {key: value for key, value in vars(instance).items() if value is not None and key != 'block_commit'}
        else:
            return {key: value for key, value in vars(instance).items() if value is not None and key not in ['block_commit', 'success', 'rowcount']}
