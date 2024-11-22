import pandas as pd

from stimula.service.diff_to_executor import DiffToExecutor
from stimula.stml.stml_parser import StmlParser


# test diff_sql method
def test_diff_executor(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))

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
    insert, update, delete = DiffToExecutor().diff_executor(mapping, (inserts, updates, deletes))

    expected_insert = ('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'name': 'Jane Austen', 'title': 'Pride and Prejudice'})
    expected_update = ('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Joseph Heller', 'title': 'Pride and Prejudice'})
    expected_delete = ('delete from books where books.title = :title', {'title': 'Pride and Prejudice'})

    # compare
    assert (insert.query, insert.params) == expected_insert
    assert (update.query, update.params) == expected_update
    assert (delete.query, delete.params) == expected_delete
