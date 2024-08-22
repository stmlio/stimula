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
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
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
        [1, 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name returning bookid', 'Pride and Prejudice', 'Jane Austen', '12345', None, None, None],
        [1, 'insert into ir_model_data (name, module, model, res_id) values (:name_1, :module, :model, :res_id)', None, None, '12345', 'netsuite_books', 'books', 7]
    ],
        columns=['rows', 'sql', 'title', 'name', 'name_1', 'module', 'model', 'res_id']
    ).astype(dtypes)

    assert df.equals(expected)
