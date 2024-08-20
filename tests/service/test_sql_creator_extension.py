import pandas as pd

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.sql_creator import InsertSqlCreator, UpdateSqlCreator

'''
This script tests sql creators for extension tables. An extension table is a table that is used to extend another table with additional attributes.
This is similar to how a table can depend on a lookup table, but instead, a table does not depend on its extension table. The extension table has a foreign key to the table it extends.
Platforms typically have one or more generic extension table that can be used to extend any other table.

One simple example in odoo is the ir_model_data table. This table is used to store the external id of any record in the system. 
This script tests generating queries for inserts, updates, and deletes for the ir_model_data table.
'''


def test_insert_sql_creator_external_id(books, meta):
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    diff = pd.DataFrame([
        [0, 'Pride and Prejudice', '12345'],
    ], columns=['__line__', 'title[unique=true]', 'bookid(name)'])

    # get queries
    inserts = InsertSqlCreator().create_sql(mapping, diff)
    actual = list(inserts)[0].queries()

    expected = [('insert into books(title) select :title returning id', {'name': '12345', 'title': 'Pride and Prejudice'}),
                ('insert into ir_model_data (name, module, model, res_id) values (:name, :module, :model, :res_id)',
                 {'model': 'books', 'module': 'netsuite_books', 'name': '12345', 'res_id': None})]

    # compare
    assert actual == expected


def test_insert_sql_creator_external_id_unique(books, meta, ir_model_data):
    # test that we can detect a required insert based on unique external id
    table_name = 'books'
    header = 'title, bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books: unique=true]'
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    # 'Emma' exists, but with external id '11111'
    diff = pd.DataFrame([
        [0, 'Emma', '11112'],
    ], columns=['__line__', 'title', 'bookid(name)[unique=true]'])

    # get queries
    inserts = InsertSqlCreator().create_sql(mapping, diff)
    actual = list(inserts)[0].queries()

    expected = [('insert into books(title) select :title returning id', {'name': '11112', 'title': 'Emma'}),
                ('insert into ir_model_data (name, module, model, res_id) values (:name, :module, :model, :res_id)',
                 {'model': 'books', 'module': 'netsuite_books', 'name': '11112', 'res_id': None})]

    # compare
    assert actual == expected


def test_update_sql_creator_unmodified_external_id(books, meta):
    # test we can update a record, even if there's an external id that is not marked as unique and was not modified
    table_name = 'books'
    header = 'title[unique=true], authorid(name), bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    diff = pd.DataFrame([
        [pd.Series([0]), 'Pride and Prejudice', 'Joseph Heller', 'Jane Austen'],
    ], columns=['__line__', ('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other')])

    # get queries
    updates = UpdateSqlCreator().create_sql(mapping, diff)
    actual = list(updates)[0].queries()

    expected = [('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})]

    # compare
    assert actual == expected

def test_update_sql_creator_unique_external_id(books, meta):
    # test we can update a record, identified by external id
    table_name = 'books'
    header = 'title, authorid(name), bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books: unique=true]'
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    diff = pd.DataFrame([
        [pd.Series([0]), 'Pride and Prejudice', 'Joseph Heller', 'Jane Austen', '11111'],
    ], columns=['__line__', ('title', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other'), ('bookid(name)[unique=true]', '')])

    # get queries
    updates = UpdateSqlCreator().create_sql(mapping, diff)
    actual = list(updates)[0].queries()

    expected = [("update books set authorid = authors.author_id from authors, ir_model_data where books.bookid = ir_model_data.res_id and authors.name = :name and ir_model_data.name = :name_1 and ir_model_data.module = 'netsuite_books' and ir_model_data.model = 'books'", {'name': 'Joseph Heller', 'name_1': '11111'})]

    # compare
    assert actual == expected

