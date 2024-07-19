from stimula.header.csv_header_parser import HeaderParser
from stimula.header.header_merger import HeaderMerger
from stimula.header.odoo_header_parser import OdooHeaderParser


def test_merge(books, meta, cr, lexer):
    table = 'books'
    default_mapping = OdooHeaderParser(meta, cr).parse('books')

    header = 'title[unique=true], authorid(name:birthyear)'
    requested_mapping = HeaderParser(meta, table).parse_csv(header)

    merged_mapping = HeaderMerger().merge(default_mapping, requested_mapping)

    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid', 'type': 'integer'}], 'in-use': True, 'primary-key': True},
        {'attributes': [{'name': 'title', 'type': 'text'}], 'in-use': True, 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'authorid', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
            {'name': 'name', 'type': 'text'},
            {'name': 'birthyear', 'type': 'integer'}
        ]}}], 'in-use': True, 'enabled': True},
        {'attributes': [{'name': 'description', 'type': 'text'}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'seriesid', 'type': 'integer', 'foreign-key': {'table': 'books', 'name': 'bookid', 'attributes': [
            {'name': 'title', 'type': 'text'},
        ]}}]},
    ]}

    assert merged_mapping == expected


def test_merge_empty_column_in_requested_header(books, meta, cr, lexer):
    table = 'books'
    default_mapping = OdooHeaderParser(meta, cr).parse('books')

    header = 'title[unique=true],'
    requested_mapping = HeaderParser(meta, table).parse_csv(header)

    merged_mapping = HeaderMerger().merge(default_mapping, requested_mapping)

    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid', 'type': 'integer'}], 'in-use': True, 'primary-key': True},
        {'attributes': [{'name': 'title', 'type': 'text'}], 'in-use': True, 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'authorid', 'type': 'integer', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
            {'name': 'name', 'type': 'text'},
        ]}}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'description', 'type': 'text'}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'seriesid', 'type': 'integer', 'foreign-key': {'table': 'books', 'name': 'bookid', 'attributes': [
            {'name': 'title', 'type': 'text'},
        ]}}]},
    ]}

    assert merged_mapping == expected


def test_merge_primary_key_as_unique_column(books, meta, cr, lexer):
    # test that if a primary key is the only unique column, then the merge maintains the primary-key attribute
    table = 'properties'
    default_mapping = OdooHeaderParser(meta, cr).parse(table)

    header = 'property_id[unique=true], name'
    requested_mapping = HeaderParser(meta, table).parse_csv(header)

    merged_mapping = HeaderMerger().merge(default_mapping, requested_mapping)

    expected = {'table': 'properties', 'columns': [
        {'attributes': [{'name': 'property_id', 'type': 'integer'}], 'enabled': True, 'unique': True, 'primary-key': True},
        {'attributes': [{'name': 'bytea', 'type': 'bytea'}]},
        {'attributes': [{'name': 'date', 'type': 'date'}]},
        {'attributes': [{'name': 'decimal', 'type': 'numeric'}]},
        {'attributes': [{'name': 'float', 'type': 'double precision'}]},
        {'attributes': [{'name': 'jsonb', 'type': 'jsonb'}]},
        {'attributes': [{'name': 'name', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'number', 'type': 'integer'}]},
        {'attributes': [{'name': 'timestamp', 'type': 'timestamp'}]},
        {'attributes': [{'name': 'value', 'type': 'text'}]}
    ]}

    assert merged_mapping == expected


def test_merge_maps():
    # test completeness and precedence
    a = {'a': 'a', 'b': 'b'}
    b = {'a': 'A', 'c': 'c'}
    c = HeaderMerger()._merge_maps(a, b)
    # acb and abc are both fine
    expected = {'a': 'A', 'c': 'c', 'b': 'b'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)


def test_merge_maps_sort_1():
    # test sorting
    a = {'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd', 'e': 'e'}
    b = {'c': 'C', 'b': 'B'}
    c = HeaderMerger()._merge_maps(a, b)
    # acbde and cabde are both fine
    expected = {'a': 'a', 'c': 'C', 'b': 'B', 'd': 'd', 'e': 'e'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)


def test_merge_maps_sort_2():
    # test sorting
    a = {'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd'}
    b = {'a': 'A', 'd': 'D', 'b': 'B'}
    c = HeaderMerger()._merge_maps(a, b)
    expected = {'a': 'A', 'c': 'c', 'd': 'D', 'b': 'B'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)
