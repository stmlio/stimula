import pandas as pd
from numpy import NaN

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.diff_to_sql import DiffToSql


# test diff_sql method
def test_diff_sql(books, meta, lexer):
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, 'books').parse(lexer.tokenize(header))

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

    expected_inserts = [('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'name': 'Jane Austen', 'title': 'Pride and Prejudice'})]
    expected_updates = [('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})]
    expected_deletes = [('delete from books where books.title = :title', {'title': 'Pride and Prejudice'})]

    # compare
    assert list(inserts) == expected_inserts
    assert list(updates) == expected_updates
    assert list(deletes) == expected_deletes
