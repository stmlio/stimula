"""
This script defines an Invoker class intended for local invocations of the Stimula API.

Author: Romke Jonker
Email: romke@rnadesign.net
"""

import psycopg2
from sqlalchemy import create_engine

from stimula.service.auth import Auth
from stimula.service.context import cnx_context
from stimula.service.db import DB


class Invoker:
    def __init__(self, secret_key, host, port):
        self._auth = LocalAuth(secret_key, host, port)
        self._db = DB()

    def set_context(self, token):
        # set context for processing of this request
        cnx_context.cnx, cnx_context.cr = self._auth.validate_token(token)

        # also create sqlalchemy engine, bec/ that's what pandas needs
        cnx_context.engine = create_engine('postgresql://', creator=lambda: cnx_context.cnx)

    def auth(self, database, username, password):
        return self._auth.authenticate(database, username, password)

    def list(self, filter):
        return self._db.get_tables(filter)

    def mapping(self, table):
        return self._db.get_header_csv(table)

    def count(self, table, header, query):
        return self._db.get_count(table, header, query)

    def get_table(self, table, header, query):
        return self._db.get_table_as_csv(table, header, query)

    def post_table(self, table, header, query, contents, skiprows, insert, update, delete, execute, commit, format, deduplicate):
        if format == None or format == 'diff':
            post_result = self._db.post_table_get_diff(table, header, query, contents, skiprows=skiprows, insert=insert, update=update, delete=delete, execute=execute, commit=commit, deduplicate=deduplicate)
            return '\n\n'.join([df.to_csv(index=False) for df in post_result])
        elif format == 'sql':
            # get diff, create sql, execute if requested and return a single data frame
            post_result = self._db.post_table_get_sql(table, header, query, contents, skiprows=skiprows, insert=insert, update=update, delete=delete, execute=execute, commit=commit, deduplicate=deduplicate)
            # convert df to response body, use double quotes where needed
            return post_result.to_csv(index=False, quotechar="\"")


class LocalAuth(Auth):
    # set the secret key during instantiation
    def __init__(self, secret_key, host, port):
        super().__init__(secret_key)
        self._host = host
        self._port = port

    def _validate_submitted_credentials(self, database, username, password):
        # create connection url
        url = f"postgresql://{username}@{self._host}:{self._port}/{database}"

        # use psycopg2 to connect to the database
        cnx = psycopg2.connect(url, password=password)

        # create cursor
        cr = cnx.cursor()

        # test connection
        cr.execute("SELECT 1")

        return username

    def _validate_token_credentials(self, database, uid, password):
        # use psycopg2 to connect to the database
        cnx = psycopg2.connect(dbname=database, user=uid, password=password, host=self._host, port=self._port)

        # create cursor
        cr = cnx.cursor()

        # test connection
        cr.execute("SELECT 1")

        # return connection and cursor objects. With psycopg2, it's best to re-use the connection and cursor objects
        return cnx, cr
