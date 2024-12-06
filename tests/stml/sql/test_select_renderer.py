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
    # test that for a non-unique extension, we do a left outer join to return all records, also those without the extension record
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = "select books.title, ir_model_data.name from books left join ir_model_data on books.bookid = ir_model_data.res_id and ir_model_data.model = 'books' and ir_model_data.module = 'netsuite_books' order by books.title"
    assert result == expected


def test_extension_in_foreign_table(books, model_enricher, context, ir_model_data):
    # test that for a non-unique extension in a joined table, we do a left outer join to return all records, also those without the extension record
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_authors])'

    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = ("select books.title, ir_model_data.name from books "
                "left join authors on books.authorid = authors.author_id "
                "left join ir_model_data on authors.author_id = ir_model_data.res_id and ir_model_data.model = 'authors' and ir_model_data.module = 'netsuite_authors' order by books.title")
    assert result == expected


def test_extension_unique(books, model_enricher, context, ir_model_data):
    # test that for a unique extension, we do an inner join to only return records that have the matching extension record
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books: unique=true]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = "select books.title, ir_model_data.name from books join ir_model_data on books.bookid = ir_model_data.res_id and ir_model_data.model = 'books' and ir_model_data.module = 'netsuite_books' order by books.title, ir_model_data.name"
    assert result == expected


def test_extension_in_foreign_table_unique(books, model_enricher, context, ir_model_data):
    # test that for a unique extension in a joined table, we do an inner join to only return records that have the matching extension record
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_authors])[unique=true]'

    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = SelectRenderer().render(mapping)
    expected = ("select books.title, ir_model_data.name from books "
                "left join authors on books.authorid = authors.author_id "
                "join ir_model_data on authors.author_id = ir_model_data.res_id and ir_model_data.model = 'authors' and ir_model_data.module = 'netsuite_authors' order by books.title, ir_model_data.name")
    assert result == expected


def test_select_json_field(books, model_enricher):
    # test that we can select a json field
    table_name = 'properties'
    header = 'name[unique=true], jsonb[key=en_US]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = "select properties.name, properties.jsonb->>'en_US' from properties order by properties.name"
    assert result == expected


def test_select_json_field_in_reference(books, model_enricher):
    # test that we can select a json field in a reference. Odoo uses this for company specific properties.
    # the query must cast the json field to an integer to join it with the target table
    table_name = 'properties'
    header = 'name[unique=true], jsonb(title)[key=1: table=books: target-name=bookid]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = "select properties.name, books.title from properties left join books on cast(properties.jsonb->>'1' as integer) = books.bookid order by properties.name"
    assert result == expected
