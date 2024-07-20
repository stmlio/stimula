import pandas as pd
from numpy import NaN

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.diff_to_sql import DiffToSql

'''
This script tests extension tables. An extension table is a table that is used to extend another table with additional attributes.
This is similar to how a table can depend on a lookup table, but instead, a table does not depend on its extension table. The extension table has a foreign key to the table it extends.
Platforms typically have one or more generic extension table that can be used to extend any other table.

One simple example in odoo is the ir_model_data table. This table is used to store the external id of any record in the system. 
This script tests generating queries for inserts, updates, and deletes for the ir_model_data table.
'''


def test_db_external_id(books, meta):
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    inserts = pd.DataFrame([
        ['Pride and Prejudice', '12345'],
    ],
        columns=['title[unique=true]', 'bookid(name)'])
    updates = pd.DataFrame([
        ['Pride and Prejudice', 'Joseph Heller', 'Jane Austen'],
    ],
        columns=[('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other')])
    deletes = pd.DataFrame([
        ['Pride and Prejudice', 'Jane Austen'],
    ],
        columns=['title[unique=true]', 'authorid(name)'])

    # get queries
    inserts, updates, deletes = DiffToSql().diff_sql(mapping, (inserts, updates, deletes))

    expected_insert_0 = ('insert into books(title) select :title returning id', {'name': '12345', 'title': 'Pride and Prejudice'})
    expected_insert_1 = ('insert into ir_model_data (name, module, model, res_id) values (:name, :module, :model, :res_id)',
                         {'model': 'books', 'module': 'netsuite_books', 'name': '12345', 'res_id': None})
    expected_updates = [('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})]
    expected_deletes = [('delete from books where books.title = :title', {'title': 'Pride and Prejudice'})]

    # compare
    assert inserts[0].query, inserts[0].params == expected_insert_0
    assert inserts[0].dependent_query == expected_insert_1
    # assert list(updates) == expected_updates
    # assert list(deletes) == expected_deletes
