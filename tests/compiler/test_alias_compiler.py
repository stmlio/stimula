from stimula.compiler.alias_compiler import AliasCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_columns(books, lexer, meta):
    table = 'books'
    header = 'title, price'
    result = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected

def test_modifiers(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[x=1: y=2]'
    result = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'x': '1', 'y': '2', 'enabled': True}
    ]}
    assert result == expected

def test_multiple_attributes(books, lexer, meta):
    table = 'books'
    header = 'bookid:title[unique=true], price'
    result = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid', 'parameter': 'bookid', 'type': 'integer'}, {'name': 'title', 'parameter': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected

def test_foreign_key(books, lexer, meta):
    table = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    expected = {'table': 'books', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'foreign-key': {'table': 'authors', 'alias': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'name', 'parameter': 'name', 'type': 'text'},
                {'name': 'publisherid', 'foreign-key': {'table': 'publishers', 'alias': 'publishers', 'name': 'publisher_id', 'attributes': [
                    {'name': 'publishername', 'parameter': 'publishername', 'type': 'text'},
                    {'name': 'country', 'parameter': 'country', 'type': 'text'}
                ]}},
                {'name': 'birthyear', 'parameter': 'birthyear', 'type': 'integer'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected

def test_alias(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    result = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'seriesid', 'foreign-key': {'table': 'books', 'alias': 'books_1', 'name': 'bookid', 'attributes': [
                {'name': 'title', 'parameter': 'title_1', 'type': 'text'}
            ]}}
        ], 'enabled': True}
    ]}
    assert result == expected
