import pytest

from stimula.header.csv_header_parser import HeaderParser


def test_empty(meta, lexer):
    header = ''
    result = HeaderParser(meta, 'books').parse_csv(header)
    expected = {'table': 'books'}
    assert result == expected


def test_table_not_found(meta, lexer):
    header = ''
    with pytest.raises(ValueError, match="Table 'booksxxx' not found"):
        HeaderParser(meta, 'booksxxx').parse_csv(header)


def test_columns(books, lexer, meta):
    table = 'books'
    header = 'title, price'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected


def test_columns_not_found(books, lexer, meta):
    # test that an error is raised when a column is not found in the table
    table = 'books'
    header = 'title, pricexxx'
    with pytest.raises(ValueError, match="Column 'pricexxx' not found in table 'books'"):
        HeaderParser(meta, table).parse_csv(header)


def test_columns_not_found_with_skip(books, lexer, meta):
    # test that an error is ignored if the missing column is marked as skip=true, so we can read the column from CSV and use it in an expression
    table = 'books'
    header = 'title, pricexxx[skip=true]'
    HeaderParser(meta, table).parse_csv(header)


def test_modifiers(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[x=1: y=2]'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'x': '1', 'y': '2', 'enabled': True}
    ]}
    assert result == expected


def test_quoted_modifiers(books, lexer, meta):
    table = 'books'
    header = 'price[a="$=1": b="$>=2": c="$ like \'%abc%\'"]'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'x': '1', 'y': '2', 'enabled': True}
    ]}
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'enabled': True, 'a': '$=1', 'b': '$>=2', 'c': "$ like '%abc%'"}
    ]}
    assert result == expected


def test_modifiers_wrong_separator(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[x=1, y=2]'
    with pytest.raises(ValueError, match="Parse error"):
        HeaderParser(meta, table).parse_csv(header)


def test_multiple_attributes(books, lexer, meta):
    table = 'books'
    header = 'bookid:title[unique=true], price'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid', 'type': 'integer'}, {'name': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected


def test_foreign_key(books, lexer, meta):
    table = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'name', 'type': 'text'},
                {'name': 'publisherid', 'foreign-key': {'table': 'publishers', 'name': 'publisher_id', 'attributes': [
                    {'name': 'publishername', 'type': 'text'},
                    {'name': 'country', 'type': 'text'}
                ]}},
                {'name': 'birthyear', 'type': 'integer'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected


def test_foreign_key_not_found(books, lexer, meta):
    table = 'books'
    header = 'authorid(name:birthyear:publisherid(publishername:countryxxx))'
    with pytest.raises(ValueError, match="Column 'countryxxx' not found in table 'publishers'"):
        HeaderParser(meta, table).parse_csv(header)


def test_reference_column_not_found(books, lexer, meta):
    table = 'books'
    header = 'authorxxx(name)'
    with pytest.raises(ValueError, match="Column 'authorxxx' not found in table 'books'"):
        HeaderParser(meta, table).parse_csv(header)


def test_default_value_header(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[default-value=10], description[default-value="this is a book"]'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'default-value': '10', 'enabled': True},
        {'attributes': [{'name': 'description', 'type': 'text'}], 'default-value': 'this is a book', 'enabled': True}
    ], }
    assert result == expected


def test_escaped_strings(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], "price[default-value=10]", "description[default-value=""this, is a book""]"'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [{'name': 'price', 'type': 'numeric'}], 'default-value': '10', 'enabled': True},
        {'attributes': [{'name': 'description', 'type': 'text'}], 'default-value': 'this, is a book', 'enabled': True}
    ], }
    assert result == expected


def test_extension_header(books, meta, ir_model_data):
    table = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'bookid', 'foreign-key': {'extension': True, 'table': 'ir_model_data', 'name': 'res_id', 'qualifier': 'netsuite_books', 'attributes': [
                {'name': 'name', 'type': 'varchar'},
            ]}}
        ], 'enabled': True}
    ], }
    assert result == expected


def test_extension_in_foreign_table(books, meta, ir_model_data):
    table = 'books'
    header = 'title[unique=true], authorid(author_id(name))[table=ir_model_data: name=res_id: qualifier=netsuite_authors]'
    result = HeaderParser(meta, table).parse_csv(header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'authorid', 'foreign-key': {'table': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'author_id', 'foreign-key': {'extension': True, 'table': 'ir_model_data', 'name': 'res_id', 'qualifier': 'netsuite_authors', 'attributes': [
                    {'name': 'name', 'type': 'varchar'},
                ]}}
            ]}}
        ], 'enabled': True}
    ], }
    assert result == expected
