from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.select_renderer import SelectRenderer
from stimula.stml.stml_parser import StmlParser


def test_simple_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books.price from books order by books.title'
    assert result == expected


def test_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert result == expected


def test_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books_1.title from books left join books as books_1 on books.seriesid = books_1.bookid order by books.title'
    assert result == expected


def test_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title), seriesid(seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books_1.title, books_3.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books.seriesid = books_2.bookid left join books as books_3 on books_2.seriesid = books_3.bookid order by books.title'
    assert result == expected


def test_compact_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books_1.title || \':\' || books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid order by books.title'
    assert result == expected


def test_order_by_foreign_key(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books_1.title || \':\' || books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid order by books.title, books_1.title, books_2.title'
    assert result == expected


def test_filter_clause(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true: filter="$ like \'abc%\'"], price[filter="$ > 10"]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books.price from books where books.title like \'abc%\' and books.price > 10 order by books.title'
    assert result == expected


def test_foreign_key_filter_clause(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(seriesid(title[filter="$ like \'abc%\'"]))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, books_2.title from books left join books as books_1 on books.seriesid = books_1.bookid left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title like \'abc%\' order by books.title'
    assert result == expected


def test_extension(books, model_enricher, context, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = "select books.title, ir_model_data.name from books join ir_model_data on books.bookid = ir_model_data.res_id and ir_model_data.model = 'books' and ir_model_data.module = 'netsuite_books' order by books.title"
    assert result == expected


def test_extension_in_foreign_table(books, model_enricher, context, ir_model_data):
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_authors])'

    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = ("select books.title, ir_model_data.name from books "
                "left join authors on books.authorid = authors.author_id "
                "join ir_model_data on authors.author_id = ir_model_data.res_id and ir_model_data.model = 'authors' and ir_model_data.module = 'netsuite_authors' order by books.title")
    assert result == expected
