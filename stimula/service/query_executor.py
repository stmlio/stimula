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


class Executor(ABC):
    def __init__(self, line_number, operation_type, table_name, context):
        self.line_number = line_number
        self.operation_type = operation_type
        self.table_name = table_name
        self.context = context

    @abstractmethod
    def execute(self, cursor):
        pass

    '''
    Called when user sets execute=False. Purpose is to inspect the queries and parameters that would be executed.
    '''
    @abstractmethod
    def fake_execute(self):
        pass

    @abstractmethod
    def queries(self):
        pass

    def _replace_placeholders(self, query):
        # replace :xyz with %(xyz)s using regex
        # but make sure to not replace the '::text' type cast in to_jsonb(:parameter::text)
        return re.sub(r'(?<!:):(\w+)', r'%(\1)s', query)


class SimpleQueryExecutor(Executor):
    def __init__(self, line_number, operation_type, table_name, query, params, context):
        super().__init__(line_number, operation_type, table_name, context)
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
            return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self.query, self.params, self.context, error=error)

        # Get the number of affected rows
        rowcount = cursor.rowcount

        # verify row was affected
        if rowcount == 0:
            error = 'No row was affected'
            return ExecutionResult(self.line_number, self.operation_type, False, rowcount, self.table_name, self.query, self.params, self.context, error=error)

        # verify no more than one row was affected
        if rowcount > 1:
            # we must not commit the transaction
            error = "More than one row was affected, do not commit."
            return ExecutionResult(self.line_number, self.operation_type, False, rowcount, self.table_name, self.query, self.params, self.context, error, True)

        result = ExecutionResult(self.line_number, self.operation_type, True, rowcount, self.table_name, self.query, self.params, self.context)
        return result

    def fake_execute(self):
        return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self.query, self.params, self.context)

# This class is used to store input rows that failed to compile into a query
class FailedQueryExecutor(Executor):
    def __init__(self, line_number, operation_type, table_name, context, error):
        super().__init__(line_number, operation_type, table_name, context)
        self.error = error

    def queries(self):
        return []

    def execute(self, cursor):
        return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, None, {}, self.context, error=self.error)

    def fake_execute(self):
        return ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, None, {}, self.context, error=self.error)

'''
This class allows for dependent queries, where the result of the first query is used as a parameter in the second query.
This is useful for extensions, such as the ir_model_data table in Odoo.
'''
class DependentQueryExecutor(Executor):
    def __init__(self, line_number, operation_type, table_name, context, initial_query, dependent_query):
        super().__init__(line_number, operation_type, table_name, context)
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
        rowcount_0 = cursor.rowcount

        # verify row was affected
        if rowcount_0 == 0:
            _logger.warning("No row was affected. Query: %s, Params: %s" % (query_0, params_0))
            execution_result = ExecutionResult(self.line_number, self.operation_type, False, rowcount_0, self.table_name, self.query, params_0, self.context)
            # skip execution of dependent query
            return execution_result

        # verify no more than one row was affected
        if rowcount_0 > 1:
            # raise exception, bec/ we must not commit the transaction
            raise ValueError("More than one row was affected. Inserts: %s, Query: %s, Params: %s" % (rowcount_0, query_0, params_0))

        execution_result = ExecutionResult(self.line_number, self.operation_type, True, rowcount_0, self.table_name, self.query, params_0, self.context)

        # replace ':' style place holders with '%' style
        query_1 = self.dependent_query[0]
        params_1 = self.dependent_query[1]
        params_1['res_id'] = result[0]

        cursor.execute(self._replace_placeholders(query_1), params_1)
        rowcount_1 = cursor.rowcount

        # create execution result of dependent query
        dependent_execution_result = ExecutionResult(self.line_number, self.operation_type, True, rowcount_1, self.table_name, query_1, params_1, self.context)
        execution_result.dependent_execution_result = dependent_execution_result

        return execution_result

    def fake_execute(self):
        execution_result = ExecutionResult(self.line_number, self.operation_type, False, 0, self.table_name, self.query, self.params, self.context)
        dependent_execution_result = ExecutionResult(self.line_number, self.operation_type, True, 0, self.table_name, self.dependent_query[0], self.dependent_query[1], self.context)
        execution_result.dependent_execution_result = dependent_execution_result
        return execution_result



class OperationType(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    def __str__(self):
        return self.value

    def __repr__(self):
        return "'" + self.value + "'"


class ExecutionResult:
    def __init__(self, line_number, operation_type, success, rowcount, table_name, query, params, context, error=None, block_commit=False):
        # convert numpy int64 to int
        self.line_number = int(line_number) if line_number is not None else None
        if not isinstance(operation_type, OperationType):
            raise ValueError(f"operation_type must be an instance of OperationType Enum, not {type(operation_type)}")
        self.operation_type = operation_type
        self.success = success
        self.rowcount = rowcount
        self.table_name = table_name
        self.query = query
        self.params = params
        self.context = context
        self.error = error
        self.block_commit = block_commit
        self.dependent_execution_result = None

    def __str__(self):
        return f"Type: {self.operation_type}, Success: {self.success}, Rowcount: {self.rowcount}, Table: {self.table_name}, Query: {self.query}, Params: {self.params}"

    def report(self, execute):
        # pandas stores empty input values as nan. Replace NaN values with empty strings.
        self.params = {k: '' if pd.isna(v) else v for k, v in self.params.items()}

        if execute:
            return [{key: value for key, value in vars(self).items() if value is not None and key not in ['block_commit', 'dependent_execution_result']}]
        else:
            return [{key: value for key, value in vars(self).items() if value is not None and key not in ['block_commit', 'success', 'rowcount', 'dependent_execution_result']}]
