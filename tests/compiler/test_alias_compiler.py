from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser


def test_columns(books, meta):
    table = 'books'
    header = 'title, price'
    result = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table, header)))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected

def test_modifiers(books, meta):
    table = 'books'
    header = 'title[unique=true], price[x=1: y=2]'
    result = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table, header)))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'x': '1', 'y': '2', 'enabled': True}
    ]}
    assert result == expected

def test_multiple_attributes(books, meta):
    table = 'books'
    header = 'bookid:title[unique=true], price'
    result = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table, header)))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'bookid', 'parameter': 'bookid', 'type': 'integer'}, {'name': 'title', 'parameter': 'title', 'type': 'text'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price', 'parameter': 'price', 'type': 'numeric'}], 'enabled': True}]}
    assert result == expected

def test_foreign_key(books, meta):
    table = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table, header)))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'type': 'integer', 'foreign-key': {'table': 'authors', 'alias': 'authors', 'name': 'author_id', 'attributes': [
                {'name': 'name', 'parameter': 'name', 'type': 'text'},
                {'name': 'publisherid', 'type': 'integer', 'foreign-key': {'table': 'publishers', 'alias': 'publishers', 'name': 'publisher_id', 'attributes': [
                    {'name': 'publishername', 'parameter': 'publishername', 'type': 'text'},
                    {'name': 'country', 'parameter': 'country', 'type': 'text'}
                ]}},
                {'name': 'birthyear', 'parameter': 'birthyear', 'type': 'integer'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected

def test_alias(books, meta):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    result = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table, header)))
    expected = {'table': 'books', 'primary-key': 'bookid', 'columns': [
        {'attributes': [{'name': 'title', 'parameter': 'title', 'type': 'text'}], 'enabled': True, 'unique': True},
        {'attributes': [
            {'name': 'seriesid', 'type': 'integer', 'foreign-key': {'table': 'books', 'alias': 'books_1', 'name': 'bookid', 'attributes': [
                {'name': 'title', 'parameter': 'title_1', 'type': 'text'}
            ]}}
        ], 'enabled': True}
    ]}
    assert result == expected
