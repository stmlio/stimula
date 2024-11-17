import pandas as pd
import pytest


def test_post_table_update_non_empty_string_to_empty(db, books, context):
    body = '''
        Emma,  
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'Emma', '']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_update_null_string_to_empty(db, books, context):
    body = '''
        Catch-22,  
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    assert df.size == 0


def test_post_table_update_null_string_to_non_empty(db, books, context):
    body = '''
        Catch-22, "Fifty years after its original publication, Catch-22 remains a cornerstone of American literature"
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'Catch-22', 'Fifty years after its original publication, Catch-22 remains a cornerstone of American literature']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_update_empty_string_to_empty(db, books, context):
    body = '''
        David Copperfield,  
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    assert df.size == 0


def test_post_table_update_empty_string_to_non_empty(db, books, context):
    body = '''
        David Copperfield, David Copperfield is the story of a young man's adventures
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'David Copperfield', 'David Copperfield is the story of a young man\'s adventures']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_update_empty_numeric_to_empty(db, books, context):
    body = '''
        David Copperfield,  
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], price', None, body, update=True, execute=True)

    assert df.size == 0


def test_post_table_update_empty_numeric_to_non_empty(db, books, context):
    body = '''
        David Copperfield, 10.99
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], price', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'price': pd.Float64Dtype()}
    expected = pd.DataFrame([
        [1, 'update books set price = :price where books.title = :title', 'David Copperfield', 10.99]
    ],
        columns=['rows', 'sql', 'title', 'price']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_update_non_empty_numeric_to_empty(db, books, context):
    body = '''
        Emma,  
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], price', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'price': pd.Int64Dtype()}
    expected = pd.DataFrame([
        [1, 'update books set price = :price where books.title = :title', 'Emma', None]
    ],
        columns=['rows', 'sql', 'title', 'price']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_duplicate_name(db, books, context):
    body = '''
        Emma,,  
    '''
    with pytest.raises(ValueError, match="Duplicate column names are not supported: price"):
        db.post_table_get_sql('books', 'title[unique=true], price, price', None, body, update=True, execute=True)


def test_post_table_update_varchar_with_number(db, books, context):
    body = '''
        Emma, 12345
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'Emma', '12345']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_table_update_varchar_with_number_and_empty(db, books, context):
    # test that reading CSV from the request does not convert a number into a float, even if the column contains an empty value.
    body = '''
        Emma, 12345
        David Copperfield, 
    '''
    df = db.post_table_get_sql('books', 'title[unique=true], description', None, body, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'Emma', '12345']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)


def test_post_NA_string_value(db, books, context):
    # test that posting NA as a string value is not interpreted as a missing value
    body = '''
        key 1, NA
    '''
    df = db.post_table_get_sql('properties', 'name[unique=true], value', None, body, insert=True, execute=True)

    assert list(df.columns) == ['rows', 'errors', 'sql', 'name', 'value']

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'name': 'string', 'value': 'string'}
    expected = pd.DataFrame([
        [1, 'insert into properties(name, value) select :name, :value', 'key 1', 'NA']
    ],
        columns=['rows', 'sql', 'name', 'value']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)

def test_post_table_with_filter(db, books, context):
    # test that updating with filter does not effect rows that do not match the filter
    body = '''
        Emma, 12345
    '''
    df = db.post_table_get_sql('books', 'title[unique=true: filter="$ = \'Emma\'"], description', None, body, update=True, insert=True, delete=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'title': 'string', 'description': 'string'}
    expected = pd.DataFrame([
        [1, 'update books set description = :description where books.title = :title', 'Emma', '12345']
    ],
        columns=['rows', 'sql', 'title', 'description']
    ).astype(dtypes)

    df = df.drop(columns=['errors'])
    assert df.equals(expected)

def test_exception_when_all_columns_empty(db, books, context):
    # validate that an exception is raised when all columns are empty
    header = ','
    body = '''
        Emma,  
    '''
    # expect exception
    with pytest.raises(ValueError, match="At least one column header must not be empty"):
        db.post_table_get_sql('books', header, None, body)

