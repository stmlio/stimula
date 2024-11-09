import pytest

from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser


def test_empty(jsonrpc_model_service):
    table_name = 'res.partner'
    header = ''
    result = ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'res.partner', 'primary-key': 'id'}
    assert result == expected


def test_table_not_found(jsonrpc_model_service):
    table_name = 'booksxxx'
    header = ''
    with pytest.raises(ValueError, match="Table 'booksxxx' not found"):
        ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))


def test_columns(books, jsonrpc_model_service):
    table_name = 'res.partner'
    header = 'name, color'
    result = ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'res.partner', 'primary-key': 'id', 'columns': [
        {'attributes': [{'name': 'name', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'color', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected


def test_columns_not_found(jsonrpc_model_service):
    # test that an error is raised when a column is not found in the table
    table_name = 'res.partner'
    header = 'name, pricexxx'
    with pytest.raises(ValueError, match="Column 'pricexxx' not found in table 'res.partner'"):
        ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))


def test_columns_not_found_with_skip(jsonrpc_model_service):
    # test that an error is ignored if the missing column is marked as skip=true, so we can read the column from CSV and use it in an expression
    table_name = 'res.partner'
    header = 'name, pricexxx[skip=true]'
    ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))


def test_foreign_key(books, jsonrpc_model_service):
    table_name = 'res.partner'
    header = 'company_id(name:partner_id(name:email):email)'
    result = ModelCompiler(jsonrpc_model_service).compile(StmlParser().parse_csv(table_name, header))
    expected = {'table': 'res.partner', 'primary-key': 'id', 'columns': [
        {'attributes': [
            {'name': 'company_id', 'type': 'integer', 'foreign-key': {'table': 'res.company', 'name': 'id', 'attributes': [
                {'name': 'name', 'type': 'text'},
                {'name': 'partner_id', 'type': 'integer', 'foreign-key': {'table': 'res.partner', 'name': 'id', 'attributes': [
                    {'name': 'name', 'type': 'text'},
                    {'name': 'email', 'type': 'text'}
                ]}},
                {'name': 'email', 'type': 'text'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected


def _test_extension_header(books, model_compiler, ir_model_data):
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


def _test_extension_in_foreign_table(books, model_compiler, ir_model_data):
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
