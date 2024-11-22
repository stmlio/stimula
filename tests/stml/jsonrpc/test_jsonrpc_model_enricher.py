import pytest

from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.model_enricher import ModelEnricher
from stimula.stml.stml_parser import StmlParser


def test_empty(jsonrpc_model_service):
    table_name = 'res.partner'
    header = ''
    result = ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('res.partner', primary_key='id')
    assert result == expected


def test_table_not_found(jsonrpc_model_service):
    table_name = 'booksxxx'
    header = ''
    with pytest.raises(ValueError, match="Table 'booksxxx' not found"):
        ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))


def test_columns(books, jsonrpc_model_service):
    table_name = 'res.partner'
    header = 'name, color'
    result = ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('res.partner', primary_key='id', attributes=[
        Attribute('name', type='text', enabled=True),
        Attribute('color', type='numeric', enabled=True)
    ])
    assert result == expected


def test_columns_not_found(jsonrpc_model_service):
    # test that an error is raised when a column is not found in the table
    table_name = 'res.partner'
    header = 'name, pricexxx'
    with pytest.raises(ValueError, match="Column 'pricexxx' not found in table 'res.partner'"):
        ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))


def test_columns_not_found_with_skip(jsonrpc_model_service):
    # test that an error is ignored if the missing column is marked as skip=true, so we can read the column from CSV and use it in an expression
    table_name = 'res.partner'
    header = 'name, pricexxx[skip=true]'
    ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))


def test_foreign_key(jsonrpc_model_service):
    table_name = 'res.partner'
    header = 'company_id(name:partner_id(name:email):email)'
    result = ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('res.partner', primary_key='id', attributes=[
        Reference('company_id', table='res.company', target_name='id', enabled=True, attributes=[
            Attribute('name', type='text'),
            Reference('partner_id', table='res.partner', target_name='id', attributes=[
                Attribute('name', type='text'),
                Attribute('email', type='text')
            ]),
            Attribute('email', type='text')
        ])
    ])

    assert result == expected


def _test_extension_header(jsonrpc_model_service, ir_model_data):
    table_name = 'res.partner'
    header = 'email[unique=true], id(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books]'
    result = ModelEnricher(jsonrpc_model_service).enrich(StmlParser().parse_csv(table_name, header))

    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', type='text', enabled=True, unique=True),
        Reference('bookid', extension=True, table='ir_model_data', target_name='res_id', qualifier='netsuite_books', enabled=True, attributes=[
            Attribute('name', type='varchar')
        ])
    ])

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
