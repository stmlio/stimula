import numpy
import pandas as pd
import pytest
from numpy import int64, nan

from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser
from stimula.service.query_executor import OperationType
from stimula.service.sql_creator import InsertSqlCreator, UpdateSqlCreator, DeleteSqlCreator


def test_create_sql(meta, books):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    inserts = pd.DataFrame([
        ['Pride and Prejudice', 0, 'Jane Austen'],
    ],
        columns=['title[unique=true]', '__line__', 'authorid(name)']
    )
    result = list(InsertSqlCreator().create_executors(mapping, inserts))

    expected = ('insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                {'title': 'Pride and Prejudice', 'name': 'Jane Austen'})
    assert result[0].query, result[0].params == expected


def test_create_sql_multiple_update_rows(meta, books):
    # verify that it can create multiple rows with different columns
    table_name = 'books'
    header = 'title[unique=true], authorid(name), description'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create multi index series with self/other columns
    columns = ['__line__', ('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other'), ('description', 'self'), ('description', 'other')]

    updates = pd.DataFrame([
        [pd.Series([0]), 'Pride and Prejudice', 'Charles Dickens', 'Jane Austen', nan, nan],
        [pd.Series([1]), 'David Copperfield', nan, nan, 'A novel by Charles Dickens, narrated by ...', nan],
    ],
        columns=columns
    )
    updates = list(UpdateSqlCreator().create_executors(mapping, updates))

    expected = [
        ('update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name', {'name': 'Charles Dickens', 'title': 'Pride and Prejudice'}),
        ('update books set description = :description where books.title = :title', {'title': 'David Copperfield', 'description': 'A novel by Charles Dickens, narrated by ...'})
    ]
    assert [(u.query, u.params) for u in updates] == expected


def test_create_sql_row_insert(meta, books):
    # test that it creates an insert sql query and a value dict
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    row = pd.Series([0, 'Pride and Prejudice', 'Jane Austen'], index=['__line__', 'title[unique=true]', 'authorid(name)'])

    executor = InsertSqlCreator()._prepare_and_create_executor(mapping, row, 1)
    expected = (OperationType.INSERT, 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name',
                {'title': 'Pride and Prejudice', 'name': 'Jane Austen'})
    assert executor.query, executor.params == expected


def test_create_sql_row_insert_skip_empty_column(books, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    row = pd.Series([0, 'Pride and Prejudice', ''], index=['__line__', 'title[unique=true]', 'authorid(name)'])
    executor = InsertSqlCreator()._prepare_and_create_executor(mapping, row, 1)

    expected = (OperationType.INSERT, 'insert into books(title) select :title', {'title': 'Pride and Prejudice'})

    assert executor.query, executor.params == expected


def test_create_sql_row_insert_skip_nan(books, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    row = pd.Series([0, 'Pride and Prejudice', nan], index=['__line__', 'title[unique=true]', 'authorid(name)'])
    executor = InsertSqlCreator()._prepare_and_create_executor(mapping, row, 1)

    expected = (OperationType.INSERT, 'insert into books(title) select :title', {'title': 'Pride and Prejudice'})

    assert executor.query, executor.params == expected


def test_create_sql_row_insert_empty_row_must_fail(books, meta):
    table_name = 'books'
    header = 'title, authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    row = pd.Series([0, nan, nan], index=['__line__', 'title', 'authorid(name)'])

    with pytest.raises(AssertionError):
        InsertSqlCreator()._prepare_mapping_and_values(mapping, row)


def test_create_sql_row_update(books, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # test that it creates an update sql query and a value dict
    row = pd.Series([(0,), 'Pride and Prejudice', 'Joseph Heller', 'Jane Austen'], index=['__line__', ('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other')])
    executor = UpdateSqlCreator()._prepare_and_create_executor(mapping, row, 1)

    expected = (OperationType.UPDATE, 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name',
                {'title': 'Pride and Prejudice', 'name': 'Joseph Heller'})

    assert executor.query, executor.params == expected


def test_create_sql_row_update_no_changes(books, meta):
    with pytest.raises(AssertionError):
        # test that it raises an exception if no non-unique attributes were updated
        table_name = 'books'
        header = 'title[unique=true], authorid(name)'
        mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
        row = pd.Series(['Pride and Prejudice', 'Jane Austen', 'Jane Austen'], index=[('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other')])
        UpdateSqlCreator()._prepare_mapping_and_values(mapping, row)


def test_create_sql_row_delete(books, meta):
    table_name = 'books'
    # test that it creates a delete sql query and a value dict
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    row = pd.Series(['Pride and Prejudice', 'Jane Austen'], index=['title[unique=true]', 'authorid(name)'])

    executor = DeleteSqlCreator()._prepare_and_create_executor(mapping, row, 1)

    expected = (OperationType.DELETE, 'delete from books where books.title = :title',
                {'title': 'Pride and Prejudice'})
    assert executor.query, executor.params == expected


def test_delete_sql_split_columns(books, meta):
    table_name = 'books'
    header = 'title:authorid[unique=true], description'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    row = pd.Series(['Pride and Prejudice:1'], index=['title:authorid[unique=true]'])
    executor = DeleteSqlCreator()._prepare_and_create_executor(mapping, row, 1)

    expected = (OperationType.DELETE, 'delete from books where books.title = :title and books.authorid = :authorid',
                {'title': 'Pride and Prejudice', 'authorid': 1})
    assert executor.query, executor.params == expected


def test_create_unique_value_dict(books, meta):
    # test that unique headers are used as keys
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    row = pd.Series(['Pride and Prejudice', 'Jane Austen'], index=['title[unique=true]', 'authorid(name)'])

    result = InsertSqlCreator()._create_unique_value_dict(mapping, row)
    expected = {'title[unique=true]': 'Pride and Prejudice'}
    assert result == expected


def test_create_non_unique_value_dict(books, meta):
    # test that non-unique headers are used as keys in the key-value map
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    row = pd.Series(['Pride and Prejudice', 'Jane Austen'], index=['title[unique=true]', 'authorid(name)'])

    result = InsertSqlCreator()._create_non_unique_value_dict(mapping, row)
    expected = {'authorid(name)': 'Jane Austen'}
    assert result == expected


def test_create_non_unique_value_dict_for_insert(books, meta):
    # test that empty values are ignored in the key-value map
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    row = pd.Series(['Pride and Prejudice', nan, None], index=['title[unique=true]', 'year', 'price'])

    result = InsertSqlCreator()._create_non_unique_value_dict(mapping, row)
    expected = {}
    assert result == expected


def test_filter_mapping(books, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    # create value dict
    value_dict = {'authorid(name)': 'Jane Austen'}

    result = InsertSqlCreator()._filter_mapping(mapping, value_dict)
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'type': 'integer', 'foreign-key': {'attributes': [{'name': 'name', 'type': 'text'}], 'name': 'author_id', 'table': 'authors'}}
        ], 'enabled': True}
    ]}
    assert result == expected


def test_map_parameter_names_with_values(books, meta):
    # test that non-unique headers are used as keys
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    # create series with column names
    parameter_names = [('title',), ('name',)]
    value_dict = {'title[unique=true]': 'Pride and Prejudice', 'authorid(name)': 'Jane Austen'}

    result = InsertSqlCreator()._map_parameter_names_with_values(mapping, parameter_names, value_dict)
    expected = {('title',): 'Pride and Prejudice', ('name',): 'Jane Austen'}
    assert result == expected


def test_split_columns():
    # create parameter names and value dict
    parameter_names = [('title',), ('name', 'year')]
    value_dict = {('title',): 'Pride and Prejudice', ('name', 'year'): 'Jane Austen:1813'}

    result = InsertSqlCreator()._split_columns(parameter_names, value_dict)
    expected = {'title': 'Pride and Prejudice', 'name': 'Jane Austen', 'year': 1813}
    assert result == expected


def test_split_value():
    # create parameter names and value dict
    parameter_name = ('name', 'year')
    value = 'Jane Austen:1813'

    result = InsertSqlCreator()._split_value(parameter_name, value)
    expected = {'name': 'Jane Austen', 'year': 1813}
    assert result == expected


def test_split_value_single():
    # create parameter names and value dict
    parameter_name = ('year',)
    value = 1813

    result = InsertSqlCreator()._split_value(parameter_name, value)
    expected = {'year': 1813}
    assert result == expected


def test_split_value_json():
    # test that splitting a value string that contains json does not split on the json colon
    parameter_name = ('year', 'json')
    value = '1813:{"key 1": "value 1"}'

    result = InsertSqlCreator()._split_value(parameter_name, value)
    expected = {'year': 1813, 'json': {"key 1": "value 1"}}
    assert result == expected


def test_clean_values_in_dict():
    # test that it removes leading and trailing whitespace
    assert InsertSqlCreator()._clean_values_in_dict({'name': '  Jane Austen  '}) == {'name': 'Jane Austen'}
    # test that it converts int64 to int
    assert InsertSqlCreator()._clean_values_in_dict({'year': int64(1813)}) == {'year': 1813}
    # test that it converts a np.float64 into a Float
    price = InsertSqlCreator()._clean_values_in_dict({'price': numpy.float64(12.34)})
    assert type(price['price']) == float
    # test that it converts multiple values at once
    assert InsertSqlCreator()._clean_values_in_dict({'name': '  Jane Austen  ', 'year': int64(1813)}) == {'name': 'Jane Austen', 'year': 1813}
    # test that it converts numpy bool to python bool
    result = InsertSqlCreator()._clean_values_in_dict({'execute': numpy.True_})['execute']
    # check that result is of type string, bec/ that's how psycopg represents booleans
    assert type(result) == str


def test_split_diff_self_other():
    diff = pd.Series(
        ['Pride and Prejudice', 'Jane Austen', 'Joseph Heller'],
        index=[('title[unique=true]', ''), ('authorid(name)', 'self'), ('authorid(name)', 'other')])

    self, other = UpdateSqlCreator()._split_diff_self_other(diff)

    expected_self = pd.Series(
        ['Pride and Prejudice', 'Jane Austen'],
        index=['title[unique=true]', 'authorid(name)'])
    expected_other = pd.Series(
        ['Pride and Prejudice', 'Joseph Heller'],
        index=['title[unique=true]', 'authorid(name)'])

    assert self.equals(expected_self)
    assert other.equals(expected_other)


def test_is_value_modified():
    assert UpdateSqlCreator()._is_value_modified('a', 'b')
    assert UpdateSqlCreator()._is_value_modified('a', 1)
    assert UpdateSqlCreator()._is_value_modified('a', nan)
    assert UpdateSqlCreator()._is_value_modified('a', None)
    assert not UpdateSqlCreator()._is_value_modified('', nan)
    assert not UpdateSqlCreator()._is_value_modified('', None)
