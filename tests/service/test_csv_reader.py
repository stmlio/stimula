import os
import re

import pandas as pd
import pytest

from stimula.header.csv_header_parser import HeaderParser
from stimula.service.csv_reader import CsvReader

csv_reader = CsvReader()
def test_read_from_request(db, books, meta):
    table = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch XIII, Joseph Heller
        David Copperfield, Charles Dickens
        Good as Gold, Joseph Heller
        Anna Karenina, Joseph Heller
        A Christmas Carol, Charles Dickens
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    dtypes = {'title': 'string', 'authorid(name)': 'string'}
    expected = pd.DataFrame([
        ['Emma', 0, 'Jane Austen'],
        ['War and Peace', 1, 'Leo Tolstoy'],
        ['Catch XIII', 2, 'Joseph Heller'],
        ['David Copperfield', 3, 'Charles Dickens'],
        ['Good as Gold', 4, 'Joseph Heller'],
        ['Anna Karenina', 5, 'Joseph Heller'],
        ['A Christmas Carol', 6, 'Charles Dickens']
    ],
        columns=['title', '__line__', 'authorid(name)'],
    ).astype(dtypes).set_index('title')

    assert df.equals(expected)


def test_read_from_request_trailing_space(db, books, meta):
    # test that trailing spaces are removed from the input
    table = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    body = '''
        Emma , Jane Austen 
    '''
    df = csv_reader.read_from_request(mapping, body, 0)

    dtypes = {'title': 'string', 'authorid(name)': 'string'}
    expected = pd.DataFrame([
        ['Emma', 0, 'Jane Austen'],
    ],
        columns=['title', '__line__', 'authorid(name)'],
    ).astype(dtypes).set_index('title')

    assert df.equals(expected)


def test_read_from_request_detect_duplicate(db, books, meta):
    # verify that duplicates in the input halts the import
    table = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    body = '''
        Emma, Jane Austen
        War and Peace, Leo Tolstoy
        Catch XIII, Joseph Heller
        Emma, Leo Tolstoy
    '''
    with pytest.raises(Exception, match=re.escape("Duplicates found: {'title[unique=true]': 'Emma'}")):
        csv_reader.read_from_request(mapping, body, 0)


def test_post_script(db):
    # create dataframe
    df = pd.DataFrame([['Emma', 'Jane Austen']], columns=['title', 'author'])

    # find absolute path for post_script.py, search in subfolders so it works in tests
    script_path = _find_file('..', 'post_script.py')

    # call execute_post_script
    result = csv_reader._execute_post_script(df, script_path)

    # assert that the script has transposed the dataframe
    expected = df.T

    assert result.equals(expected)


def _find_file(folder, file):
    # find absolute path for script, search in subfolders, so it works in tests
    for root, dirs, files in os.walk(folder):
        if file in files:
            return os.path.join(root, file)
    raise FileNotFoundError(f"Could not find {file}")
