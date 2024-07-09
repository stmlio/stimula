from stimula.header.odoo_header_parser import OdooHeaderParser


def test_publishers(books, meta, cr):
    header = OdooHeaderParser(meta, cr).parse('publishers')
    expected = {'table': 'publishers', 'columns': [
        {'attributes': [{'name': 'publisher_id', 'type': 'integer'}], 'primary-key': True, 'enabled': True},
        {'attributes': [{'name': 'country', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'publishername', 'type': 'text'}], 'unique': True, 'enabled': True}
    ]}
    assert header == expected


def test_authors(books, meta, cr):
    header = OdooHeaderParser(meta, cr).parse('authors')
    expected = {'table': 'authors', 'columns': [
        {'attributes': [{'name': 'author_id', 'type': 'integer'}], 'primary-key': True, 'in-use': True},
        {'attributes': [{'name': 'name', 'type': 'text'}], 'unique': True, 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'birthyear', 'type': 'integer'}], 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'publisherid', 'type': 'integer', 'foreign-key': {'table': 'publishers', 'name': 'publisher_id', 'attributes': [
            {'name': 'publishername', 'type': 'text'},
            {'name': 'country', 'type': 'text'}
        ]}}]}
    ]}
    assert header == expected


def test_books(books, meta, cr):
    header = OdooHeaderParser(meta, cr).parse('books')
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid', 'type': 'integer'}], 'in-use': True, 'primary-key': True},
        {'attributes': [{'name': 'title', 'type': 'text'}], 'in-use': True, 'unique': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'authorid', 'type': 'integer', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
            {'name': 'name', 'type': 'text'},
        ]}}], 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'description', 'type': 'text'}], 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'seriesid', 'type': 'integer', 'foreign-key': {'table': 'books', 'name': 'bookid', 'attributes': [
            {'name': 'title', 'type': 'text'},
        ]}}]},
    ]}
    assert header == expected


def test_properties_empty(books, meta, cr):
    # test all columns are enabled if the table is empty
    header = OdooHeaderParser(meta, cr).parse('properties')
    expected = {'table': 'properties', 'columns': [
        {'attributes': [{'name': 'property_id', 'type': 'integer'}], 'enabled': True, 'primary-key': True, 'unique': True, 'default': True},
        {'attributes': [{'name': 'bytea', 'type': 'bytea'}], 'enabled': True},
        {'attributes': [{'name': 'date', 'type': 'date'}], 'enabled': True},
        {'attributes': [{'name': 'decimal', 'type': 'numeric'}], 'enabled': True},
        {'attributes': [{'name': 'float', 'type': 'double precision'}], 'enabled': True},
        {'attributes': [{'name': 'jsonb', 'type': 'jsonb'}], 'enabled': True},
        {'attributes': [{'name': 'name', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'number', 'type': 'integer'}], 'enabled': True},
        {'attributes': [{'name': 'timestamp', 'type': 'timestamp'}], 'enabled': True},
        {'attributes': [{'name': 'value', 'type': 'text'}], 'enabled': True}
    ]}
    assert header == expected


def test_properties_no_unique_constraints(books, meta, cr):
    # test that primary key is included if the table has no unique constraints
    cr.execute("INSERT INTO properties(name, value) VALUES('key 1', 'value 1')")
    header = OdooHeaderParser(meta, cr).parse('properties')
    expected = {'table': 'properties', 'columns': [
        {'attributes': [{'name': 'property_id', 'type': 'integer'}], 'in-use': True, 'primary-key': True, 'unique': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'bytea', 'type': 'bytea'}]},
        {'attributes': [{'name': 'date', 'type': 'date'}]},
        {'attributes': [{'name': 'decimal', 'type': 'numeric'}]},
        {'attributes': [{'name': 'float', 'type': 'double precision'}]},
        {'attributes': [{'name': 'jsonb', 'type': 'jsonb'}]},
        {'attributes': [{'name': 'name', 'type': 'text'}], 'in-use': True, 'default': True, 'enabled': True},
        {'attributes': [{'name': 'number', 'type': 'integer'}]},
        {'attributes': [{'name': 'timestamp', 'type': 'timestamp'}]},
        {'attributes': [{'name': 'value', 'type': 'text'}], 'in-use': True, 'default': True, 'enabled': True}
    ]}
    assert header == expected
