import os
import re

import pandas as pd
import pytest

from stimula.service.csv_reader import CsvReader
from stimula.stml.stml_parser import StmlParser

csv_reader = CsvReader()


def test_read_from_request(db, books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
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


def test_read_from_request_trailing_space(db, books, model_enricher):
    # test that trailing spaces are removed from the input
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
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


def test_read_from_request_detect_duplicate(db, books, model_enricher):
    # verify that duplicates in the input halts the import
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
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


def test_concat():
    # test that the @concat function decorates and concatenates values
    table_name = 'any'
    header = 'a, b, c, "xyz[exp=""@concat(\':\', True, a, b, c)""]"'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        a, b, c,
         , b, c,
         ,  , c,
         ,  ,  ,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, 'a', 'b', 'c', '"a":"b":"c"'],
        [1, '', 'b', 'c', '"b":"c"'],
        [2, '', '', 'c', '"c"'],
        [3, '', '', '', ''],
    ]

    assert df.values.tolist() == expected


def test_concat():
    # test that the @concat function can deal with constants
    table_name = 'any'
    header = 'a, b, c, "xyz[exp=""@concat(\':\', True, a, \'suffix\')""]"'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        a, b, c,
         , b, c,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, 'a', 'b', 'c', '"a":"suffix"'],
        [1, '', 'b', 'c', '"suffix"'],
    ]

    assert df.values.tolist() == expected


def test_afas_file_name():
    # test that the @afas_file_name function replaces special characters the AFAS way
    table_name = 'any'
    header = 'name, some_other_field, "fixed_name[exp=""@afas_file_name(name)""]"'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        /#&:?*<>%+, some other value,
        ~-@!$_\', some other other value,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, '/#&:?*<>%+', 'some other value', '_2F_23_26_3A_3F_2A_3C_3E_25_2B'],
        [1, '~-@!$_\'', 'some other other value', '_7E_2D_40_21_24_5F_27'],
    ]

    assert df.values.tolist() == expected


def test_fallback():
    # test that the @fallback function falls back to the first non-empty value
    table_name = 'any'
    header = 'a, b, c, "xyz[exp=""@fallback(a, b, c)""]"'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        a, b, c,
         , b, c,
         ,  , c,
         ,  ,  ,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, 'a', 'b', 'c', 'a'],
        [1, '', 'b', 'c', 'b'],
        [2, '', '', 'c', 'c'],
        [3, '', '', '', ''],
    ]

    assert df.values.tolist() == expected


def test_filter_src():
    # test that we can filter out rows based on a condition
    table_name = 'any'
    header = 'a, b[filter-src="b != \'1\'"]'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        1, b,
        2, 1,
        3, "1",
        4, ,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, '1', 'b'],
        [1, '4', ''],
    ]

    assert df.values.tolist() == expected


def test_filter_src_empty():
    # test that the we can filter out empty values
    table_name = 'any'
    header = 'a, b[filter-src="b != \'\'"]'
    mapping = StmlParser().parse_csv(table_name, header)
    body = '''
        1, b,
        2,    
        3, "1",
        4, ,
    '''

    df = csv_reader.read_from_request(mapping, body, 0)

    expected = [
        [0, '1', 'b'],
        [1, '3', '1'],
    ]

    assert df.values.tolist() == expected
