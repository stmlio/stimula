import pandas as pd
from numpy import NaN

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.diff_to_sql import DiffToSql


# test diff_sql method
def test_diff_sql(books, meta, lexer):
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, 'books').parse_csv(header)

    inserts = pd.DataFrame([
        ['Pride and Prejudice', 'Jane Austen'],
    ],
        columns=['title[unique=true]', 'authorid(name)'])
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

    expected_insert = ('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'name': 'Jane Austen', 'title': 'Pride and Prejudice'})
    expected_update = ('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})
    expected_delete = ('delete from books where books.title = :title', {'title': 'Pride and Prejudice'})

    # compare
    assert (inserts[0].query, inserts[0].params) == expected_insert
    assert (updates[0].query, updates[0].params) == expected_update
    assert (deletes[0].query, deletes[0].params) == expected_delete
