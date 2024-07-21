import pandas as pd
import pytest
from numpy import NaN, nan, isnan

from stimula.service.query_executor import SimpleQueryExecutor


def test_tables(books, db, context):
    tables = db.get_tables()
    expected = {'name': 'authors', 'count': 4}
    assert expected in tables


def test_filtered_tables(books, db, context):
    tables = db.get_tables(filter='oo')
    expected = [{'name': 'books', 'count': 6}]
    assert tables == expected


def test_get_select_statement(books, db, context):
    query = db.get_select_statement('books', 'title[unique=true], authorid(name)')
    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert query == expected


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


def test_get_table_split_column(db, books, cnx):
    df, _ = db.get_table('books', 'title:authorid(name)')
    dtypes = {'title:authorid(name)': 'string'}
    expected = pd.DataFrame([
        ['Emma:Jane Austen'],
        ['War and Peace:Leo Tolstoy'],
        ['Catch-22:Joseph Heller'],
        ['David Copperfield:Charles Dickens'],
        ['Good as Gold:Joseph Heller'],
        ['Anna Karenina:Leo Tolstoy']
    ],
        columns=['title:authorid(name)']
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

    expected_create = [['Catch XIII', 'Joseph Heller'], ['A Christmas Carol', 'Charles Dickens']]
    expected_update = [['Anna Karenina', 'Joseph Heller', 'Leo Tolstoy']]
    expected_delete = [['Catch-22', 'Joseph Heller']]

    assert create.values.tolist() == expected_create
    assert update.values.tolist() == expected_update
    assert delete.values.tolist() == expected_delete


def test_post_table_get_diff_with_default_values(db, books):
    body = '''
        Emma,, 
    '''
    create, update, delete = db.post_table_get_diff('books', 'title[unique=true], authorid(name)[default-value="Jane Austen"], price[default-value=20]', None, body, update=True)

    expected_update = [['Emma', 20, 10.99]]

    assert update.values.tolist() == expected_update


def test_post_table_get_diff_with_expression(db, books):
    header = 'title[unique=true],a_x[skip=true], b[skip=true], "description[exp=""a_x.str.cat(b, sep=\' \')""]"'

    body = '''
        War and Peace,the new,description, 
    '''
    create, update, delete = db.post_table_get_diff('books', header, None, body, update=True)

    expected_update = [['War and Peace', 'the new description', '']]

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
        ['delete from books where books.title = :title', 'Catch-22', NaN],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'Catch XIII', 'Joseph Heller'],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'A Christmas Carol', 'Charles Dickens'],
        ['update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', 'Anna Karenina', 'Joseph Heller']
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
        ['delete from books where books.title = :title', 'Catch-22', NaN],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'Catch XIII', 'Joseph Heller'],
        ['insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', 'A Christmas Carol', 'Charles Dickens'],
        ['update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', 'Anna Karenina', 'Joseph Heller']
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
    with pytest.raises(Exception, match="Header must have at least one unique column"):
        db.post_table_get_sql('books', 'title[unique=false], authorid(name)', None, body, update=True)


def test_execute_sql_no_commit(db, books, context):
    sql = [
        SimpleQueryExecutor('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'title': 'Catch XIII', 'name': 'Joseph Heller'}),
        SimpleQueryExecutor('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'title': 'Witches', 'name': 'Charles Dickens'}),
        SimpleQueryExecutor('update books set authorid = authors.author_id from authors where title = :title and authors.name = :name', {'title': 'Anna Karenina', 'name': 'Leo Tolstoy'}),
        SimpleQueryExecutor('delete from books where title = :title', {'title': 'Catch-22'})
    ]
    result = db._execute_sql(sql)

    rowcounts = [r for (r, q, p) in result]
    expected = [1, 1, 1, 1]
    assert rowcounts == expected


def test_execute_sql_with_commit(db, books, context):
    sql = [
        SimpleQueryExecutor('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'title': 'Catch XIII', 'name': 'Joseph Heller'}),
        SimpleQueryExecutor('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name', {'title': 'Witches', 'name': 'Charles Dickens'}),
        SimpleQueryExecutor('update books set authorid = authors.author_id from authors where title = :title and authors.name = :name', {'title': 'Anna Karenina', 'name': 'Leo Tolstoy'}),
        SimpleQueryExecutor('delete from books where title = :title', {'title': 'Catch-22'})
    ]
    result = db._execute_sql(sql, commit=True)
    rowcounts = [r for (r, q, p) in result]
    expected = [1, 1, 1, 1]
    assert rowcounts == expected


def test_compare(db):
    # create two dataframes
    left = pd.DataFrame([['Emma', 'Jane Austen']], columns=['title', 'author'])
    right = pd.DataFrame([['Emma', None]], columns=['title', 'author'])
    _, result, _ = db._compare(left, right, insert=True, update=True, delete=True)
    expected = pd.DataFrame([
        [0, 'Jane Austen', None],
    ], columns=[('index', ''), ('author', 'self'), ('author', 'other')])
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

    expected_create = ['Pride and Prejudice', 'Jane Austen', nan]

    # check nan values separately, bec you can't compare them
    assert create.values.tolist()[0][:2] == expected_create[:2]
    assert isnan(create.values.tolist()[0][2])