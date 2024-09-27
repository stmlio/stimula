'''
This script tests scenarios involving an extension table, such as the ir_model_data table that stores external ids for Odoo models.
'''
import pandas as pd
import pytest

from stimula.header.csv_header_parser import HeaderParser


def test_insert_extension(db, books, ir_model_data):
    # test that we can insert a record into an extension table
    table_name = 'books'
    header = 'title[unique=true], authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books]'
    body = '''
        Pride and Prejudice, Jane Austen, 12345 
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, insert=True, context='my table')

    dtypes = {'sql': 'string', 'title': 'string', 'name': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning books.bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
        ['insert into ir_model_data (name, module, model, res_id) values (:name_1, :module, :model, :res_id)', None, None, '12345', 'netsuite_books', 'books', None]
    ],
        columns=['sql', 'title', 'name', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)


def test_insert_execute_extension(db, books, ir_model_data, context):
    # test that we can insert a record into an extension table and execute it
    table_name = 'books'
    header = 'title[unique=true], authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books]'
    body = '''
        Pride and Prejudice, Jane Austen, 12345 
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, insert=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'title': 'string', 'name': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        [1, 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning books.bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
        [1, 'insert into ir_model_data (name, module, model, res_id) values (:name_1, :module, :model, :res_id)', None, None, '12345', 'netsuite_books', 'books', 7]
    ],
        columns=['rows', 'sql', 'title', 'name', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)


def test_insert_unique_extension(db, books, ir_model_data, context):
    # test that we can insert a record into an extension that is unique
    table_name = 'books'
    header = 'title, authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books: unique=true]'
    body = '''
        Pride and Prejudice, Jane Austen, 12345 
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, insert=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'title': 'string', 'name': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        [1, 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning books.bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
        [1, 'insert into ir_model_data (name, module, model, res_id) values (:name_1, :module, :model, :res_id)', None, None, '12345', 'netsuite_books', 'books', 7]
    ],
        columns=['rows', 'sql', 'title', 'name', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)

def test_insert_extension_defaults(db, books, ir_model_data):
    # test that we can take defaults for table and name
    table_name = 'books'
    header = 'title[unique=true], authorid(name), bookid(name)[id=bookid: qualifier=netsuite_books]'
    body = '''
        Pride and Prejudice, Jane Austen, 12345 
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, insert=True, context='my table')

    dtypes = {'sql': 'string', 'title': 'string', 'name': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning books.bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
        ['insert into ir_model_data (name, module, model, res_id) values (:name_1, :module, :model, :res_id)', None, None, '12345', 'netsuite_books', 'books', None]
    ],
        columns=['sql', 'title', 'name', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)


def test_update_unique_extension(db, books, ir_model_data, context):
    # test that we can update a record by its unique extension
    table_name = 'books'
    header = 'title, authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books: unique=true]'
    body = '''
        Pride and Prejudice, Jane Austen, 22222 
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, update=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'title': 'string', 'name': 'string', 'name_1': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set title = :title, authorid = authors.author_id from authors, ir_model_data where books.bookid = ir_model_data.res_id and authors.name = :name and ir_model_data.name = :name_1 and ir_model_data.module = \'netsuite_books\' and ir_model_data.model = \'books\'', 'Pride and Prejudice', 'Jane Austen', '22222'],
    ],
        columns=['rows', 'sql', 'title', 'name', 'name_1']
    ).astype(dtypes)

    assert df.equals(expected)


def test_delete_execute_extension(db, books, ir_model_data, context):
    # test that we can delete records from an extension table
    table_name = 'books'
    header = 'title[unique=true], authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books]'
    body = '''
        War and Peace, Leo Tolstoy, 22222
        Catch-22, Joseph Heller, 33333
        David Copperfield, Charles Dickens, 
        Good as Gold, Joseph Heller,
        Anna Karenina, Leo Tolstoy,
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, delete=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'title': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        [1, 'delete from books where books.title = :title returning books.bookid', 'Emma', '11111', None, None, None],
        [1, 'delete from ir_model_data where name = :name_1 and module = :module and model = :model and res_id = :res_id', None, '11111', 'netsuite_books', 'books', 1]
    ],
        columns=['rows', 'sql', 'title', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)


def test_delete_unique_execute_extension(db, books, ir_model_data, context):
    # test that we can delete records from an extension table if the external id is unique
    table_name = 'books'
    header = 'title, authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books: unique=true]'
    body = '''
        War and Peace, Leo Tolstoy, 22222
        Catch-22, Joseph Heller, 33333
        David Copperfield, Charles Dickens, 44444
        Good as Gold, Joseph Heller, 55555
        Anna Karenina, Leo Tolstoy, 66666
    '''
    # post
    df = db.post_table_get_sql(table_name, header, None, body, delete=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        [1,
         'delete from books using ir_model_data where books.bookid = ir_model_data.res_id and ir_model_data.name = :name_1 and ir_model_data.module = \'netsuite_books\' and ir_model_data.model = \'books\' returning books.bookid',
         '11111', None, None, None],
        [1, 'delete from ir_model_data where name = :name_1 and module = :module and model = :model and res_id = :res_id', '11111', 'netsuite_books', 'books', 1]
    ],
        columns=['rows', 'sql', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)

def test_delete_unique_extension_skip_null(db, books, ir_model_data, context):
    # test that if we delete by extension table, it must skip records without a value in the extension table
    # this is useful, for example if we're deleting contacts from Odoo but don't want to delete admin

    # first, insert without extension
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    body = '''
        Pride and Prejudice, Jane Austen
    '''
    # post
    db.post_table_get_sql(table_name, header, None, body, insert=True, execute=True, commit=True, context='my table')

    table_name = 'books'
    header = 'title, authorid(name), bookid(name)[table=ir_model_data: id=bookid: name=res_id: qualifier=netsuite_books: unique=true]'
    body = '''
        War and Peace, Leo Tolstoy, 22222
        Catch-22, Joseph Heller, 33333
        David Copperfield, Charles Dickens, 44444
        Good as Gold, Joseph Heller, 55555
        Anna Karenina, Leo Tolstoy, 66666
    '''
    # post again, to delete Emma
    df = db.post_table_get_sql(table_name, header, None, body, delete=True, execute=True, context='my table')

    dtypes = {'rows': 'Int64', 'sql': 'string', 'name_1': 'string', 'module': 'string', 'model': 'string', 'res_id': 'Int64'}
    expected = pd.DataFrame([
        [1,
         'delete from books using ir_model_data where books.bookid = ir_model_data.res_id and ir_model_data.name = :name_1 and ir_model_data.module = \'netsuite_books\' and ir_model_data.model = \'books\' returning books.bookid',
         '11111', None, None, None],
        [1, 'delete from ir_model_data where name = :name_1 and module = :module and model = :model and res_id = :res_id', '11111', 'netsuite_books', 'books', 1]
    ],
        columns=['rows', 'sql', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)
