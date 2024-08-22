from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.update_compiler import UpdateCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_simple_query(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], price'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
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


def test_multiple_join_query(books, meta, lexer, context):
    # test multiple join statements in update query
    table = 'books'
    header = 'title[unique=true], authorid(name), seriesid(title)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set authorid = authors.author_id, seriesid = books_1.bookid from authors, books as books_1 where books.title = :title and authors.name = :name and books_1.title = :title_1'
    assert result == expected


def test_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 where books.title = :title and books_1.title = :title_1'
    assert result == expected


def test_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_2.title = :title_1'
    assert result == expected


def test_colon_separated_columns(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], authorid(name:birthyear)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name and authors.birthyear = :birthyear'
    assert result == expected


def test_compact_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_1.title = :title_1 and books_2.title = :title_2'
    assert result == expected


def test_unique_join_query(books, meta, lexer, context):
    # test update query identifying the record by a joined column
    table = 'books'
    header = 'title, authorid(name)[unique=true]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set title = :title from authors where books.authorid = authors.author_id and authors.name = :name'
    assert result == expected


def test_unique_multiple_join_query(books, meta, lexer, context):
    # test update query identifying the record by a joined column
    table = 'books'
    header = 'title, seriesid(authorid(name))[unique=true]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = 'update books set title = :title from books as books_1 left join authors on books_1.authorid = authors.author_id where books.seriesid = books_1.bookid and authors.name = :name'
    assert result == expected


def test_extension_not_unique(books, meta, context, ir_model_data):
    # extension on primary table. If it's not marked as unique, then this query would allow updating it.
    table = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = "update books set bookid = ir_model_data.res_id from ir_model_data where books.title = :title and ir_model_data.name = :name and ir_model_data.module = 'netsuite_books' and ir_model_data.model = 'books'"
    assert result == expected


def test_extension_unique(books, meta, context, ir_model_data):
    # extension on primary table. If it's marked as unique, then this query allows identifying the record by the extension column.
    table = 'books'
    header = 'title, bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books: unique=true]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = UpdateCompiler().compile(mapping)
    expected = "update books set title = :title from ir_model_data where books.bookid = ir_model_data.res_id and ir_model_data.name = :name and ir_model_data.module = 'netsuite_books' and ir_model_data.model = 'books'"
    assert result == expected
