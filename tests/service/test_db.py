import pandas as pd
import pytest
from numpy import nan, isnan

from stimula.service.query_executor import SimpleQueryExecutor, OperationType


def test_tables(books, db, context):
    tables = db.get_tables()
    expected = {'name': 'authors', 'count': 4}
    assert expected in tables


def test_filtered_tables(books, db, context):
    tables = db.get_tables(filter='oo')
    expected = [{'name': 'books', 'count': 6}]
    assert tables == expected


def test_get_table(db, books, cnx):
    df, _ = db.get_table('books', 'title[unique=true], authorid(name)')
    dtypes = {'title[unique=true]': 'string', 'authorid(name)': 'string'}
    expected = pd.DataFrame([
        ['Anna Karenina', 'Leo Tolstoy'],
        ['Catch-22', 'Joseph Heller'],
        ['David Copperfield', 'Charles Dickens'],
        ['Emma', 'Jane Austen'],
        ['Good as Gold', 'Joseph Heller'],
        ['War and Peace', 'Leo Tolstoy']
    ],
        columns=['title[unique=true]', 'authorid(name)']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_get_table_empty_columns(db, books, cnx):
    df, _ = db.get_table('books', 'title[unique=true], , authorid(name),')
    dtypes = {'title[unique=true]': 'string', '': 'string', 'authorid(name)': 'string'}
    expected = pd.DataFrame([
        ['Anna Karenina', '', 'Leo Tolstoy', ''],
        ['Catch-22', '', 'Joseph Heller', ''],
        ['David Copperfield', '', 'Charles Dickens', ''],
        ['Emma', '', 'Jane Austen', ''],
        ['Good as Gold', '', 'Joseph Heller', ''],
        ['War and Peace', '', 'Leo Tolstoy', '']
    ],
        columns=['title[unique=true]', '', 'authorid(name)', '']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_post_table_get_diff_no_changes(db, books, context):
    # verify that processing a table with no changes results in empty diffs
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch-22, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Leo Tolstoy
    '''
    create, update, delete = db.post_table_get_diff('books', 'title[unique=true], authorid(name)', None, body, insert=True, update=True, delete=True)
    assert create.empty
    assert update.empty
    assert delete.empty


def test_post_table_get_diff_with_changes(db, books):
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch XIII, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        A Christmas Carol, Charles Dickens
    '''
    create, update, delete = db.post_table_get_diff('books', 'title[unique=true], authorid(name)', None, body, insert=True, update=True, delete=True)

    expected_create = [[2, 'Catch XIII', 'Joseph Heller'], [6, 'A Christmas Carol', 'Charles Dickens']]
    expected_update = [[5, 'Anna Karenina', 'Joseph Heller', 'Leo Tolstoy']]
    expected_delete = [['Catch-22', 'Joseph Heller']]

    assert create.values.tolist() == expected_create
    assert update.values.tolist() == expected_update
    assert delete.values.tolist() == expected_delete


def test_post_table_get_diff_with_default_values(db, books):
    body = '''
        Emma,, 
    '''
    create, update, delete = db.post_table_get_diff('books', 'title[unique=true], authorid(name)[default-value="Jane Austen"], price[default-value=20]', None, body, update=True)

    expected_update = [[0, 'Emma', 20, 10.99]]

    assert update.values.tolist() == expected_update


def test_post_table_get_diff_with_expression(db, books):
    header = 'title[unique=true],a_x[skip=true], b[skip=true], "description[exp=""a_x.str.cat(b, sep=\' \')""]"'

    body = '''
        War and Peace,the new,description, 
    '''
    create, update, delete = db.post_table_get_diff('books', header, None, body, update=True)

    expected_update = [[0, 'War and Peace', 'the new description', '']]

    assert update.values.tolist() == expected_update


def test_post_table_get_diff_with_expression_on_index(db, books):
    # tests that an expression can include an index field. This requires that we convert the index value to a
    # known type other than 'object'. Because, if numexpr is installed, then it fails if the type is object.
    header = 'title[unique=true],description[exp=title]'

    body = '''
        War and Peace,a description, 
    '''
    create, update, delete = db.post_table_get_diff('books', header, None, body, update=True)

    expected_update = [[0, 'War and Peace', 'War and Peace', '']]

    assert update.values.tolist() == expected_update


def test_post_table_get_diff_with_expression_on_same_field(db, books):
    # tests that an expression can include an index field. This requires that we convert the index value to a
    # known type other than 'object'. Because, if numexpr is installed, then it fails if the type is object.
    header = 'title[unique=true],"description[exp=""title.str.cat(description, sep=\':\')""]"'

    body = '''
        War and Peace,a description, 
    '''
    create, update, delete = db.post_table_get_diff('books', header, None, body, update=True)

    expected_update = [[0, 'War and Peace', 'War and Peace:a description', '']]

    assert update.values.tolist() == expected_update


def test_post_table_get_sql_no_changes(db, books, context):
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch-22, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Leo Tolstoy
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], authorid(name)', None, body)
    assert df.empty


def test_post_table_get_sql_with_changes(db, books, context):
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch XIII, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        A Christmas Carol, Charles Dickens
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], authorid(name)', None, body, insert=True, update=True, delete=True)

    dtypes = {'sql': 'string', 'title': 'string', 'name': 'string'}
    expected = pd.DataFrame([
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'Catch XIII', 'Joseph Heller'],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'A Christmas Carol', 'Charles Dickens'],
        ['update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', 'Anna Karenina', 'Joseph Heller'],
        ['delete from books where books.title = :title', 'Catch-22', nan],
    ],
        columns=['sql', 'title', 'name']
    ).astype(dtypes)

    assert df.equals(expected)


def test_post_table_get_sql_with_changes_with_empty_columns(db, books, context):
    # validate posting a table with empty column names
    body = '''
        Emma, xxx, Jane Austen, yyy
        War and Peace, xxx, Leo Tolstoy, yyy
        Catch XIII, , Joseph Heller,
        David Copperfield, ,Charles Dickens, 
        Good as Gold, , Joseph Heller, 
        Anna Karenina, xxx, Joseph Heller, yyy
        A Christmas Carol, xxx, Charles Dickens, yyy
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], , authorid(name), ', None, body, insert=True, update=True, delete=True)

    dtypes = {'sql': 'string', 'title': 'string', 'name': 'string'}
    expected = pd.DataFrame([
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'Catch XIII', 'Joseph Heller'],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'A Christmas Carol', 'Charles Dickens'],
        ['update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', 'Anna Karenina', 'Joseph Heller'],
        ['delete from books where books.title = :title', 'Catch-22', nan],
    ],
        columns=['sql', 'title', 'name']
    ).astype(dtypes)

    assert df.equals(expected)


def test_post_table_get_sql_only_update(db, books, context):
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch XIII, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        Witches, Charles Dickens
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], authorid(name)', None, body, update=True)

    dtypes = {'sql': 'string', 'title': 'string', 'name': 'string'}
    expected = pd.DataFrame([
        ['update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', 'Anna Karenina', 'Joseph Heller']
    ],
        columns=['sql', 'title', 'name']
    ).astype(dtypes)

    assert df.equals(expected)


def test_post_table_update_with_missing_unique_column(db, books, context):
    body = '''
        Anna Karenina, Joseph Heller
    '''
    db.post_table_get_sql('books', 'title[unique=false], authorid(name)', None, body, update=True)
    # TODO: report errors in post_table_get_sql



def test_compare(db):
    # create two dataframes
    left = pd.DataFrame([[0, 'Emma', 'Jane Austen']], columns=['__line__', 'title', 'author'])
    right = pd.DataFrame([['Emma', None]], columns=['title', 'author'])
    _, result, _ = db._compare(left, right, insert=True, update=True, delete=True)
    expected = pd.DataFrame([
        [0, 0, 'Jane Austen', None],
    ], columns=[('__line__', ''), ('index', ''), ('author', 'self'), ('author', 'other')])
    assert result.equals(expected)


def test_get_header_json(books, db, context):
    json = db.get_header_json('books')
    expected = {'table-name': 'books', 'columns': [
        {'key': 'bookid', 'in-use': True, 'primary-key': True, 'type': 'integer'},
        {'key': 'title[unique=true]', 'default': True, 'enabled': True, 'in-use': True, 'type': 'text', 'unique': True},
        {'key': 'authorid(name)', 'default': True, 'enabled': True, 'in-use': True, 'type': 'text', 'foreign-key': True},
        {'key': 'description', 'default': True, 'enabled': True, 'in-use': True, 'type': 'text'},
        {'key': 'price', 'default': True, 'enabled': True, 'in-use': True, 'type': 'numeric'},
        {'key': 'seriesid(title)', 'type': 'text', 'foreign-key': True}
    ]}
    assert json == expected


def test_get_header_csv(books, db, context):
    csv = db.get_header_csv('books')
    expected = 'title[unique=true], authorid(name), description, price'
    assert csv == expected


def _test_get_header_json_by_header(books, db, context):
    # validate selecting and sorting based on supplied header
    df = db.get_header_json('books', 'title[unique=true], price, authorid(name)')

    expected = [
        {'key': 'title[unique=true]', 'type': 'text', 'in-use': True, 'unique': True, 'default': True, 'selected': True},
        {'key': 'price', 'type': 'numeric', 'in-use': True, 'default': True, 'selected': True},
        {'key': 'authorid(name)', 'type': 'text', 'in-use': True, 'default': True, 'selected': True},
        {'key': 'bookid', 'type': 'integer', 'in-use': True, 'primary-key': True},
        {'key': 'authorid', 'type': 'integer', 'in-use': True, 'foreign-key': True},
        {'key': 'description', 'type': 'text', 'in-use': True, 'default': True},
        {'key': 'seriesid', 'type': 'integer', 'foreign-key': True},
        {'key': 'seriesid(title)', 'type': 'text', 'default': True}]

    assert df == expected


def _test_get_header_json_by_custom_header(books, db, context):
    # validate selecting and sorting based on supplied header
    df = db.get_default_header_json('books', 'title[unique=true], price, authorid(name)')

    expected = [
        {'key': 'title[unique=true]', 'type': 'text', 'in-use': True, 'unique': True, 'default': True, 'selected': True},
        {'key': 'price', 'type': 'numeric', 'in-use': True, 'default': True, 'selected': True},
        {'key': 'authorid(name)', 'type': 'text', 'in-use': True, 'default': True, 'selected': True},
        {'key': 'bookid', 'type': 'integer', 'in-use': True, 'primary-key': True},
        {'key': 'authorid', 'type': 'integer', 'in-use': True, 'foreign-key': True},
        {'key': 'description', 'type': 'text', 'in-use': True, 'default': True},
        {'key': 'seriesid', 'type': 'integer', 'foreign-key': True},
        {'key': 'seriesid(title)', 'type': 'text', 'default': True}]

    assert df == expected


def test_get_count(db, books, context):
    count = db.get_count('books', None, None)
    assert count == 6


def test_get_count_with_where_clause(db, books, context):
    count = db.get_count('books', None, 'authorid = 3')
    assert count == 2


def test_get_count_with_error(db, books, context):
    with pytest.raises(Exception, match='column "abc" does not exist.*'):
        db.get_count('books', None, 'authorid = abc')


def test_post_table_padding(db, books):
    # test that it can pad with empty columns if needed
    body = '''
        Pride and Prejudice, Jane Austen
    '''
    create, update, delete = db.post_table_get_diff('books', 'title[unique=true], authorid(name), description', None, body, insert=True, update=True, delete=True)

    expected_create = [0, 'Pride and Prejudice', 'Jane Austen', nan]

    # check nan values separately, bec you can't compare them
    assert create.values.tolist()[0][:2] == expected_create[:2]
    assert isnan(create.values.tolist()[0][3])


def test_post_table_padding_with_unique_default(db, books):
    # test that it can pad with a column with default value and then promote that to unique index
    body = '''
        Emma, Jane Austen, 24.99
    '''

    # take existing description to verify it uses it in select
    default_description = "Emma Woodhouse is one of Austen's most captivating and vivid characters."

    df = db.post_table_get_sql('books', f'title[unique=true], authorid(name), price, "description[default-value=""{default_description}"": unique=true]"', None, body, update=True)

    dtypes = {'sql': 'string', 'title': 'string', 'price': 'Float64', 'description': 'string'}
    expected = pd.DataFrame([
        ['update books set price = :price where books.title = :title and books.description = :description', 'Emma', 24.99, default_description]
    ],
        columns=['sql', 'title', 'price', 'description']
    ).astype(dtypes)

    assert df.equals(expected)


def test_post_table_get_full_report(db, books, context):
    body = '''
        Emma, Jane Austen
        War and Peace, Jane Austen
        Catch XIII, Joseph Heller
        David Copperfield, Charlie Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        A Christmas Carol, Charles Dickens
    '''
    full_report = db.post_table_get_full_report('books', 'title[unique=true], authorid(name)', None, body, insert=True, update=True, delete=True, execute=True, context='my table')

    # sort rows by line number
    full_report['rows'] = sorted(full_report['rows'], key=lambda x: x.get('line_number', 0))

    expected = {
        'summary': {'commit': False,
                    'execute': True,
                    'failed': {'delete': 0, 'insert': 0, 'update': 1},
                    'rows': 7,
                    'success': {'delete': 1, 'insert': 2, 'update': 2},
                    'total': {'delete': 1, 'failed': 1, 'insert': 2, 'operations': 6, 'success': 5, 'update': 3}
                    }, 'files': [{'context': 'my table',
                                  'md5': '4d243d3873027da1af176f6e9e078f91',
                                  'size': 259,
                                  'table': 'books'}
                                 ], 'rows': [
            {'operation_type': OperationType.DELETE, 'success': True, 'rowcount': 1, 'table_name': 'books', 'context': 'my table',
             'query': 'delete from books where books.title = :title',
             'params': {'title': 'Catch-22'}},
            {'line_number': 1, 'operation_type': OperationType.UPDATE, 'success': True, 'rowcount': 1, 'table_name': 'books', 'context': 'my table',
             'query': 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name',
             'params': {'title': 'War and Peace', 'name': 'Jane Austen'}},
            {'line_number': 2, 'operation_type': OperationType.INSERT, 'success': True, 'rowcount': 1, 'table_name': 'books', 'context': 'my table',
             'query': 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
             'params': {'title': 'Catch XIII', 'name': 'Joseph Heller'}},
            {'line_number': 3, 'operation_type': OperationType.UPDATE, 'success': False, 'rowcount': 0, 'table_name': 'books', 'context': 'my table',
             'query': 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name',
             'params': {'name': 'Charlie Dickens', 'title': 'David Copperfield'},
             'error': 'No row was affected'},
            {'line_number': 5, 'operation_type': OperationType.UPDATE, 'success': True, 'rowcount': 1, 'table_name': 'books', 'context': 'my table',
             'query': 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name',
             'params': {'title': 'Anna Karenina', 'name': 'Joseph Heller'}},
            {'line_number': 6, 'operation_type': OperationType.INSERT, 'success': True, 'rowcount': 1, 'table_name': 'books', 'context': 'my table',
             'query': 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
             'params': {'title': 'A Christmas Carol', 'name': 'Charles Dickens'}},
        ]}

    # remove /summary/timestamp, because it is a timestamp
    full_report['summary'].pop('timestamp')

    assert full_report == expected


def test_post_table_get_full_report_no_execute(db, books, context):
    body = '''
        Emma, Jane Austen
        War and Peace, Jane Austen
        Catch XIII, Joseph Heller
        David Copperfield, Charlie Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        A Christmas Carol, Charles Dickens
    '''
    full_report = db.post_table_get_full_report('books', 'title[unique=true], authorid(name)', None, body, insert=True, update=True, delete=True, context='my table')
    expected_summary = {
        'execute': False, 'commit': False,
        'failed': {'delete': 1, 'insert': 2, 'update': 3},
        'rows': 7,
        'success': {'delete': 0, 'insert': 0, 'update': 0},
        'total': {'delete': 1, 'failed': 6, 'insert': 2, 'operations': 6, 'success': 0, 'update': 3}
    }

    # remove /summary/timestamp, because it is a timestamp
    full_report['summary'].pop('timestamp')

    assert full_report['summary'] == expected_summary

    rows = full_report['rows']

    assert len(rows) == 6
