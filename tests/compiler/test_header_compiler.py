from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.header_compiler import HeaderCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_empty(meta, lexer):
    header = ''
    mapping = HeaderParser(meta, 'books').parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'books'}
    assert json == expected


def test_columns(books, lexer, meta):
    table = 'books'
    header = 'title, price'
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'books', 'columns': [{'key': 'title', 'enabled': True, 'type': 'text'}, {'key': 'price', 'enabled': True, 'type': 'numeric'}]}
    assert json == expected


def test_modifiers(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[x=1: y=2]'
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'books', 'columns': [
        {'key': 'title[unique=true]', 'unique': True, 'enabled': True, 'type': 'text'},
        {'key': 'price[x=1: y=2]', 'enabled': True, 'type': 'numeric'}
    ]}
    assert json == expected


def test_modifier_with_quoted_value(books, lexer, meta):
    table = 'books'
    header = 'title[unique=true], price[filter="$ = \'abc\'"]'
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header


def test_multiple_attributes(books, lexer, meta):
    table = 'books'
    header = 'bookid:title[unique=true], price'
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'books', 'columns': [
        {'key': 'bookid:title[unique=true]', 'unique': True, 'enabled': True},
        {'key': 'price', 'enabled': True, 'type': 'numeric'}]}
    assert json == expected


def test_foreign_key(books, lexer, meta):
    table = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv(mapping)
    assert csv == header
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'books', 'columns': [
        {'key': 'authorid(name:publisherid(publishername:country):birthyear)', 'enabled': True, 'foreign-key': True}
    ]}
    assert json == expected


def test_type(books, lexer, meta):
    mapping = {'table': 'authors', 'columns': [
        {'attributes': [{'name': 'authorid', 'type': 'integer'}], 'primary-key': True, 'in-use': True},
        {'attributes': [{'name': 'name', 'type': 'text'}], 'unique': True, 'in-use': True, 'default': True},
        {'attributes': [{'name': 'birthyear', 'type': 'integer'}], 'in-use': True, 'default': True},
        {'attributes': [{'name': 'publisherid', 'type': 'integer', 'foreign-key': {'table': 'publishers', 'attributes': [
            {'name': 'publishername', 'type': 'text'},
            {'name': 'country', 'type': 'text'}
        ]}}]}
    ]}
    json = HeaderCompiler().compile_json(mapping)
    expected = {'table-name': 'authors', 'columns': [
        {'key': 'authorid', 'type': 'integer', 'in-use': True, 'primary-key': True},
        {'key': 'name[unique=true]', 'type': 'text', 'default': True, 'in-use': True, 'unique': True},
        {'key': 'birthyear', 'type': 'integer', 'default': True, 'in-use': True},
        {'key': 'publisherid(publishername:country)', 'type': 'text', 'foreign-key': True}
    ]}
    assert json == expected


def test_list(books, lexer, meta):
    table = 'books'
    header = 'title, price'
    mapping = HeaderParser(meta, table).parse_csv(header)
    list = HeaderCompiler().compile_list(mapping)
    assert list == ['title', 'price']


def test_unique_columns(books, lexer, meta):
    table = 'books'
    header = ' bookid: title [unique =true], price  '
    mapping = HeaderParser(meta, table).parse_csv(header)
    csv = HeaderCompiler().compile_csv_unique(mapping)
    assert csv == 'bookid:title[unique=true]'


def test_unique_list(books, lexer, meta):
    table = 'books'
    header = ' bookid: title [unique =true], price  '
    mapping = HeaderParser(meta, table).parse_csv(header)
    list = HeaderCompiler().compile_list_unique(mapping)
    assert list == ['bookid:title[unique=true]']


def test_non_unique_list(books, lexer, meta):
    table = 'books'
    header = ' bookid: title [unique =true], price  '
    mapping = HeaderParser(meta, table).parse_csv(header)
    list = HeaderCompiler().compile_list_non_unique(mapping)
    assert list == ['price']


def test_list_with_skip(books, lexer, meta):
    table = 'books'
    header = ' title [unique =true], price, xyz[skip=true]  '
    mapping = HeaderParser(meta, table).parse_csv(header)
    list = HeaderCompiler().compile_list(mapping)
    assert list == ['title[unique=true]', 'price']
