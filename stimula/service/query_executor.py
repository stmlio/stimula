import re
from abc import ABC, abstractmethod

import pandas as pd

'''
This class allows for different execution styles. 
In particular, it allows for dependent queries, where the result of the first query is used as a parameter in the second query.
'''


class QueryExecutor(ABC):
    @abstractmethod
    def execute(self, cursor):
        pass

    def _replace_placeholders(self, query):
        # replace :xyz with %(xyz)s using regex
        return re.sub(r':(\w+)', r'%(\1)s', query)


class SimpleQueryExecutor(QueryExecutor):
    def __init__(self, query, params):
        self.query = query
        self.params = params

    def execute(self, cursor):
        # replace ':' style place holders with '%' style
        psycopg_query = self._replace_placeholders(self.query)
        # replace NA values with None in params dictionary
        params_with_none = {k: None if pd.isna(v) else v for k, v in self.params.items()}
        # execute query
        cursor.execute(psycopg_query, params_with_none)
        # Get the number of affected rows
        result = (cursor.rowcount, self.query, self.params)
        return result


class DependentQueryExecutor(QueryExecutor):
    def __init__(self, initial_query, dependent_query):
        self.query = initial_query[0]
        self.params = initial_query[1]
        self.dependent_query = dependent_query

    def execute(self, cursor):
        # replace ':' style place holders with '%' style
        query_0 = self._replace_placeholders(self.query)

        # replace NA values with None in params dictionary
        params_0 = {k: None if pd.isna(v) else v for k, v in self.params.items()}

        cursor.execute(query_0, params_0)
        result = cursor.fetchone()
        rowcount = cursor.rowcount

        # replace ':' style place holders with '%' style
        query_1 = self._replace_placeholders(self.dependent_query[0])
        params_1 = self.dependent_query[1]
        params_1['res_id'] = result[0]

        cursor.execute(query_1, params_1)
        result = (rowcount, query_0, params_0)

        return result
