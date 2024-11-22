from enum import unique

import pytest

from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.stml_parser import StmlParser


def test_empty(model_enricher):
    table_name = 'books'
    header = ''
    result = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('books', primary_key='bookid')
    assert result == expected


def test_table_not_found(model_enricher):
    table_name = 'booksxxx'
    header = ''
    with pytest.raises(ValueError, match="Table 'booksxxx' not found"):
        model_enricher.enrich(StmlParser().parse_csv(table_name, header))


def test_columns(books, model_enricher):
    table_name = 'books'
    header = 'title, price'
    result = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', type='text', enabled=True),
        Attribute('price', type='numeric', enabled=True)
    ])
    assert result == expected


def test_columns_not_found(books, model_enricher):
    # test that an error is raised when a column is not found in the table
    table_name = 'books'
    header = 'title, pricexxx'
    with pytest.raises(ValueError, match="Column 'pricexxx' not found in table 'books'"):
        model_enricher.enrich(StmlParser().parse_csv(table_name, header))


def test_columns_not_found_with_skip(books, model_enricher):
    # test that an error is ignored if the missing column is marked as skip=true, so we can read the column from CSV and use it in an expression
    table_name = 'books'
    header = 'title, pricexxx[skip=true]'
    model_enricher.enrich(StmlParser().parse_csv(table_name, header))


def test_foreign_key(books, model_enricher):
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('books', primary_key='bookid', attributes=[
        Reference('authorid', table='authors', target_name='author_id', enabled=True, attributes=[
            Attribute('name', type='text'),
            Reference('publisherid', table='publishers', target_name='publisher_id', attributes=[
                Attribute('publishername', type='text'),
                Attribute('country', type='text')
            ]),
            Attribute('birthyear', type='integer')
        ])
    ])

    assert result == expected


def test_foreign_key_not_found(books, model_enricher):
    table_name = 'books'
    header = 'authorid(name:birthyear:publisherid(publishername:countryxxx))'
    with pytest.raises(ValueError, match="Column 'countryxxx' not found in table 'publishers'"):
        model_enricher.enrich(StmlParser().parse_csv(table_name, header))


def test_reference_column_not_found(books, model_enricher):
    table_name = 'books'
    header = 'authorxxx(name)'
    with pytest.raises(ValueError, match="Column 'authorxxx' not found in table 'books'"):
        model_enricher.enrich(StmlParser().parse_csv(table_name, header))


def test_extension_header(books, model_enricher, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target_name=res_id: qualifier=netsuite_books]'
    result = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', unique=True, type='text', enabled=True),
        Reference('bookid', extension=True, table='ir_model_data', target_name='res_id', qualifier='netsuite_books', enabled=True, attributes=[
            Attribute('name', type='varchar')
        ])
    ])
    assert result == expected


def test_extension_in_foreign_table(books, model_enricher, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name)[table=ir_model_data: target_name=res_id: qualifier=netsuite_authors])'
    result = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', unique=True, type='text', enabled=True),
        Reference('authorid', table='authors', target_name='author_id', enabled=True, attributes=[
            Reference('author_id', extension=True, table='ir_model_data', target_name='res_id', qualifier='netsuite_authors', attributes=[
                      Attribute('name', type='varchar')
            ])
        ])
    ])
    assert result == expected
