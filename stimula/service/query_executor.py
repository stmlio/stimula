import logging
import re
from abc import ABC, abstractmethod

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
    def __init__(self, query, params):
        self.query = query
        self.params = params

    def queries(self):
        return [(self.query, self.params)]

    def execute(self, cursor):
        # replace ':' style place holders with '%' style
        psycopg_query = self._replace_placeholders(self.query)
        # replace NA values with None in params dictionary
        params_with_none = {k: None if pd.isna(v) else v for k, v in self.params.items()}
        # execute query
        cursor.execute(psycopg_query, params_with_none)

        # Get the number of affected rows
        rowcount = cursor.rowcount

        # verify row was inserted
        if rowcount == 0:
            _logger.warning("No row was inserted. Query: %s, Params: %s" % (self.query, self.params))
            return (rowcount, self.query, self.params)

        # verify no more than one row was inserted
        if rowcount > 1:
            # raise exception, bec/ we must not commit the transaction
            raise ValueError("More than one row was inserted. Inserts: %s, Query: %s, Params: %s" % (rowcount, self.query, self.params))


        result = (rowcount, self.query, self.params)
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
