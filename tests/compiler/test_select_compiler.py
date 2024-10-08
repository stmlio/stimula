from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.select_compiler import SelectCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_simple_query(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], price'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books.price from books order by books.title'
    assert result == expected


def test_join_query(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert result == expected


def test_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books_1.title from books left join books as books_1 on books.seriesid = books_1.bookid order by books.title'
    assert result == expected


def test_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title), seriesid(seriesid(title))'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books_1.title, books_3.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books.seriesid = books_2.bookid left join books as books_3 on books_2.seriesid = books_3.bookid order by books.title'
    assert result == expected


def test_colon_separated_columns(books, meta, lexer, context):
    table = 'books'
    header = 'title:price[unique=true]'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = "select books.title || ':' || books.price from books order by books.title, books.price"
    assert result == expected


def test_compact_multiple_join_alias(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books_1.title || \':\' || books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid order by books.title'
    assert result == expected


def test_order_by_foreign_key(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))[unique=true]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books_1.title || \':\' || books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid order by books.title, books_1.title, books_2.title'
    assert result == expected


def test_filter_clause(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true: filter="$ like \'abc%\'"], price[filter="$ > 10"]'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books.price from books where books.title like \'abc%\' and books.price > 10 order by books.title'
    assert result == expected


def test_foreign_key_filter_clause(books, meta, lexer, context):
    table = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))[filter="$ like \'abc%\'"]'
    mapping = AliasCompiler().compile(HeaderParser(meta, table).parse_csv(header))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title like \'abc%\' order by books.title'
    assert result == expected


def test_extension(books, meta, context, ir_model_data):
    table = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: name=res_id: qualifier=netsuite_books]'
    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = "select books.title, ir_model_data.name from books join ir_model_data on books.bookid = ir_model_data.res_id and ir_model_data.model = 'books' and ir_model_data.module = 'netsuite_books' order by books.title"
    assert result == expected


def test_extension_in_foreign_table(books, meta, context, ir_model_data):
    table = 'books'
    header = 'title[unique=true], authorid(author_id(name))[table=ir_model_data: name=res_id: qualifier=netsuite_authors]'

    mapping = HeaderParser(meta, table).parse_csv(header)
    result = SelectCompiler().compile(mapping)
    expected = ("select books.title, ir_model_data.name from books "
                "left join authors on books.authorid = authors.author_id "
                "join ir_model_data on authors.author_id = ir_model_data.res_id and ir_model_data.model = 'authors' and ir_model_data.module = 'netsuite_authors' order by books.title")
    assert result == expected
