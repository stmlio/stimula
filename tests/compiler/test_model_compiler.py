import pytest

from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser


def test_empty(model_compiler):
    table_name = 'books'
    header = ''
    result = model_compiler.compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'books', 'primary-key': 'bookid'}
    assert result == expected


def test_table_not_found(model_compiler):
    table_name = 'booksxxx'
    header = ''
    with pytest.raises(ValueError, match="Table 'booksxxx' not found"):
        model_compiler.compile(StmlParser().parse_csv(table_name, header))


def test_columns(books, model_compiler):
    table_name = 'books'
    header = 'title, price'
    result = model_compiler.compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected


def test_columns_not_found(books, model_compiler):
    # test that an error is raised when a column is not found in the table
    table_name = 'books'
    header = 'title, pricexxx'
    with pytest.raises(ValueError, match="Column 'pricexxx' not found in table 'books'"):
        model_compiler.compile(StmlParser().parse_csv(table_name, header))


def test_columns_not_found_with_skip(books, model_compiler):
    # test that an error is ignored if the missing column is marked as skip=true, so we can read the column from CSV and use it in an expression
    table_name = 'books'
    header = 'title, pricexxx[skip=true]'
    model_compiler.compile(StmlParser().parse_csv(table_name, header))


def test_foreign_key(books, model_compiler):
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = model_compiler.compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'type': 'integer', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'name', 'type': 'text'},
                {'name': 'publisherid', 'type': 'integer', 'foreign-key': {'table': 'publishers', 'name': 'publisher_id', 'attributes': [
                    {'name': 'publishername', 'type': 'text'},
                    {'name': 'country', 'type': 'text'}
                ]}},
                {'name': 'birthyear', 'type': 'integer'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected


def test_foreign_key_not_found(books, model_compiler):
    table_name = 'books'
    header = 'authorid(name:birthyear:publisherid(publishername:countryxxx))'
    with pytest.raises(ValueError, match="Column 'countryxxx' not found in table 'publishers'"):
        model_compiler.compile(StmlParser().parse_csv(table_name, header))


def test_reference_column_not_found(books, model_compiler):
    table_name = 'books'
    header = 'authorxxx(name)'
    with pytest.raises(ValueError, match="Column 'authorxxx' not found in table 'books'"):
        model_compiler.compile(StmlParser().parse_csv(table_name, header))


def test_extension_header(books, model_compiler, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    result = model_compiler.compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'bookid', 'type': 'integer', 'foreign-key': {'extension': True, 'table': 'ir_model_data', 'name': 'res_id', 'qualifier': 'netsuite_books', 'attributes': [
                {'name': 'name', 'type': 'varchar'},
            ]}}
        ], 'enabled': True}
    ], }
    assert result == expected


def test_extension_in_foreign_table(books, model_compiler, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name))[table=ir_model_data: name=res_id: qualifier=netsuite_authors]'
    result = model_compiler.compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'authorid', 'type': 'integer', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'author_id', 'type': 'integer', 'foreign-key': {'extension': True, 'table': 'ir_model_data', 'name': 'res_id', 'qualifier': 'netsuite_authors', 'attributes': [
                    {'name': 'name', 'type': 'varchar'},
                ]}}
            ]}}
        ], 'enabled': True}
    ], }
    assert result == expected