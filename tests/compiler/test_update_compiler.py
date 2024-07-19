from stimula.compiler.update_compiler import UpdateCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_simple_query(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], price'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set price = :price where books.title = :title'
    assert result == expected


def test_join_query(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name'
    assert result == expected


def test_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 where books.title = :title and books_1.title = :title_1'
    assert result == expected


def test_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_2.title = :title_1'
    assert result == expected


def test_colon_separated_columns(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], authorid(name:birthyear)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name and authors.birthyear = :birthyear'
    assert result == expected


def test_compact_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_1.title = :title_1 and books_2.title = :title_2'
    assert result == expected
