from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.orm.orm_insert_renderer import OrmInsertRenderer, OrmParameterNamesRenderer
from stimula.stml.stml_parser import StmlParser


def test_simple_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = ''
    assert result == expected


def test_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select authors.author_id as authorid from authors where authors.name = :name'
    assert result == expected


def test_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select books_1.bookid as seriesid from books as books_1 where books_1.title = :title_1'
    assert result == expected


def test_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select books_1.bookid as seriesid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title = :title_1'
    assert result == expected


def test_extension_table(books, model_enricher, context):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], checksum'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select books.bookid as res_id from books where books.title = :title'
    assert result == expected


def test_colon_separated_columns(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = "select authors.author_id as authorid from authors where authors.name = :name and authors.birthyear = :birthyear"
    assert result == expected


def test_compact_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select books_1.bookid as seriesid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_1.title = :title_1 and books_2.title = :title_2'
    assert result == expected


def test_multiple_join_alias_in_different_order(books, model_enricher, context):
    '''
    column names must be in predictable order, not depending on how the compiler works internally.
    This is important, bec/ we need to match input coluns with sql query parameter names
    '''
    table_name = 'books'
    header = 'seriesid(seriesid(title): title), title[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmInsertRenderer().render(mapping)
    expected = 'select books_1.bookid as seriesid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title = :title and books_1.title = :title_1'
    assert result == expected


def test_parameter_compiler(books, model_enricher, context):
    # test the parameter names compiler
    table_name = 'books'
    header = 'seriesid(seriesid(title): title), title[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = OrmParameterNamesRenderer().render(mapping)
    # title and title_1 come from query, title_2 comes from the input
    expected = ['title_2']
    assert result == expected
