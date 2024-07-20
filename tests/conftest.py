import sys

print('sys.path')
print(sys.path)
import os

import psycopg2
import pytest
from sqlalchemy import (create_engine, MetaData)

from stimula.service.auth import Auth
from stimula.service.db import DB, cnx_context
from stimula.header.header_lexer import HeaderLexer
from stimula.header.csv_header_parser import HeaderParser


class TestAuth(Auth):
    def __init__(self, secret_key):
        super().__init__(secret_key)

    def _validate_submitted_credentials(self, database, username, password):
        pass

    def _validate_token_credentials(self, database, username, password):
        pass


@pytest.fixture
def db_params():
    # Database connection parameters
    return {
        "host": os.environ.get('HOST', 'localhost'),
        "database": os.environ.get('DATABASE', 'graph'),
        "user": os.environ.get('DB_USER', 'postgres'),
        "password": os.environ.get('PASSWORD', 'admin'),
        "port": os.environ.get('PORT', "5433"),
    }


@pytest.fixture
def url(db_params):
    # return connection string without password
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"


@pytest.fixture
def auth(db_params):
    auth = TestAuth('secret')
    auth.authenticate(db_params['database'], db_params['user'], db_params['password'])
    return auth


@pytest.fixture
def db():
    return DB()


@pytest.fixture
def eng(db_params):
    return create_engine(
        f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    )


@pytest.fixture
def meta(cnx):
    engine = create_engine('postgresql://', creator=lambda: cnx)

    # Create a MetaData instance
    metadata = MetaData()

    # Reflect the existing database schema
    metadata.reflect(bind=engine)

    return metadata


@pytest.fixture
def books(cnx):
    with cnx.cursor() as cr:
        cr.execute('DROP TABLE IF EXISTS books')
        cr.execute('DROP TABLE IF EXISTS authors')
        cr.execute('DROP TABLE IF EXISTS publishers')
        cr.execute('DROP TABLE IF EXISTS properties')
        cr.execute('''CREATE TABLE properties(
            property_id SERIAL PRIMARY KEY,
            name TEXT,
            value TEXT,
            number INTEGER,
            float FLOAT,
            decimal DECIMAL, 
            timestamp TIMESTAMP,
            date DATE,
            jsonb JSONB,
            bytea BYTEA)''')
        cr.execute('''CREATE TABLE publishers(
            publisher_id SERIAL PRIMARY KEY, 
            publishername TEXT,
            country TEXT,
            CONSTRAINT uc_publishers_name_country UNIQUE (publishername, country))''')
        cr.execute('''CREATE TABLE authors(
            author_id SERIAL PRIMARY KEY, 
            name TEXT UNIQUE,
            birthyear INTEGER,
            publisherid INTEGER,
            FOREIGN KEY(publisherid) REFERENCES publishers(publisher_id))''')
        cr.execute('''CREATE TABLE books(
            bookid SERIAL PRIMARY KEY, 
            title TEXT UNIQUE NOT NULL,
            description TEXT,
            authorid INTEGER NOT NULL, 
            seriesid INTEGER, 
            price DECIMAL, 
            FOREIGN KEY(authorid) REFERENCES authors(author_id), 
            FOREIGN KEY(seriesid) REFERENCES books(bookid))''')

        authors = ({"AuthorId": 1, "Name": "Jane Austen", "BirthYear": 1775},
                   {"AuthorId": 2, "Name": "Leo Tolstoy", "BirthYear": 1828},
                   {"AuthorId": 3, "Name": "Joseph Heller", "BirthYear": 1923},
                   {"AuthorId": 4, "Name": "Charles Dickens", "BirthYear": 1812}
                   )

        books = ({"BookId": 1, "Title": "Emma", "AuthorId": 1, "description": "Emma Woodhouse is one of Austen's most captivating and vivid characters.", "price": 10.99},
                 {"BookId": 2, "Title": "War and Peace", "AuthorId": 2, "description": '', "price": None},
                 {"BookId": 3, "Title": "Catch-22", "AuthorId": 3, "description": None, "price": None},
                 {"BookId": 4, "Title": "David Copperfield", "AuthorId": 4, "description": None, "price": None},
                 {"BookId": 5, "Title": "Good as Gold", "AuthorId": 3, "description": None, "price": None},
                 {"BookId": 6, "Title": "Anna Karenina", "AuthorId": 2, "description": None, "price": None}
                 )

        for line in authors:
            cr.execute("INSERT INTO authors(author_id, name, birthyear) VALUES(%(AuthorId)s, %(Name)s, %(BirthYear)s)", line)
            cr.execute("""select nextval('authors_author_id_seq')""")

        for line in books:
            cr.execute("INSERT INTO books(bookid, title, authorid, description, price) VALUES(%(BookId)s, %(Title)s, %(AuthorId)s, %(description)s, %(price)s)", line)
            cr.execute("""select nextval('books_bookid_seq')""")

        cnx.commit()
@pytest.fixture
def ir_model_data(cnx):
    with cnx.cursor() as cr:
        cr.execute('DROP TABLE IF EXISTS ir_model_data')
        cr.execute('''create table ir_model_data (
            id serial primary key,
            res_id integer,
            name varchar not null
                constraint ir_model_data_name_nospaces
                check ((name)::text !~~ '%% %%'::text),
            module varchar not null,
            model varchar not null);''')


        external_ids = ({"res_id": 1, "name": "11111", "module": "netsuite_books", "model": "books"},
                        {"res_id": 2, "name": "22222", "module": "netsuite_books", "model": "books"},
                        {"res_id": 3, "name": "33333", "module": "netsuite_books", "model": "books"})


        for line in external_ids:
            cr.execute("INSERT INTO ir_model_data(res_id, name, module, model) VALUES(%(res_id)s, %(name)s, %(module)s, %(model)s)", line)
            # cr.execute("""select nextval('books_bookid_seq')""")

        cnx.commit()


@pytest.fixture
def lexer():
    return HeaderLexer()


@pytest.fixture
def parser():
    return HeaderParser()


@pytest.fixture
def cnx(db_params):
    # connect to database
    cnx = psycopg2.connect(**db_params)
    # verify connection is open
    assert cnx.closed == 0
    # save in thread local
    cnx_context.cnx = cnx
    # also create sqlalchemy engine, bec/ that's what pandas needs
    cnx_context.engine = create_engine('postgresql://', creator=lambda: cnx)

    # yield connection
    yield cnx
    # verify connection is open
    assert cnx.closed == 0
    # close connection
    cnx.close()
    # verify connection is close
    assert cnx.closed == 1


@pytest.fixture
def cr(cnx):
    return cnx.cursor()


@pytest.fixture
def context(cr):
    cnx_context.cr = cr


@pytest.fixture
def test_table(cnx):
    with cnx:
        # create cursor
        with cnx.cursor() as cursor:
            # delete table if it exists
            cursor.execute("DROP TABLE IF EXISTS test")
            # execute create table statement
            cursor.execute("CREATE TABLE test (id integer)")
