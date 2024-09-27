import pandas as pd

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.diff_to_sql import DiffToSql


# test diff_sql method
def test_diff_sql(books, meta, lexer):
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, 'books').parse_csv(header)

    inserts = pd.DataFrame([
        ['Pride and Prejudice', 0, 'Jane Austen'],
    ],
        columns=['title[unique=true]', '__line__', 'authorid(name)'])
    updates = pd.DataFrame([
        ['Pride and Prejudice', pd.Series([1]), 'Joseph Heller', 'Jane Austen'],
    ],
        columns=[('title[unique=true]', ''), '__line__', ('authorid(name)', 'self'), ('authorid(name)', 'other')])
    deletes = pd.DataFrame([
        ['Pride and Prejudice', 'Jane Austen'],
    ],
        columns=['title[unique=true]', 'authorid(name)'])

    # get queries
    insert, update, delete = DiffToSql().diff_sql(mapping, (inserts, updates, deletes))

    expected_insert = ('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'name': 'Jane Austen', 'title': 'Pride and Prejudice'})
    expected_update = ('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})
    expected_delete = ('delete from books where books.title = :title', {'title': 'Pride and Prejudice'})

    # compare
    assert (insert.query, insert.params) == expected_insert
    assert (update.query, update.params) == expected_update
    assert (delete.query, delete.params) == expected_delete
