from stimula.stml.stml_parser import StmlParser
from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.insert_renderer import InsertRenderer


def test_simple_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, price) select :title, :price'
    assert result == expected


def test_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name'
    assert result == expected


def test_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, seriesid) select :title, books_1.bookid from books as books_1 where books_1.title = :title_1'
    assert result == expected


def test_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, seriesid) select :title, books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title = :title_1'
    assert result == expected


def test_colon_separated_columns(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = "insert into books(title, authorid) select :title, authors.author_id from authors where authors.name = :name and authors.birthyear = :birthyear"
    assert result == expected


def test_compact_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, seriesid) select :title, books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_1.title = :title_1 and books_2.title = :title_2'
    assert result == expected


def test_multiple_join_alias_in_different_order(books, model_enricher, context):
    '''
    column names must be in predictable order, not depending on how the compiler works internally.
    This is important, bec/ we need to match input coluns with sql query parameter names
    '''
    table_name = 'books'
    header = 'seriesid(seriesid(title): title), title[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(seriesid, title) select books_1.bookid, :title_2 from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books_2.title = :title and books_1.title = :title_1'
    assert result == expected

def test_double_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(authorid(name): seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = 'insert into books(title, seriesid) select :title, books_1.bookid from books as books_1 left join authors on books_1.authorid = authors.author_id left join books as books_2 on books_1.seriesid = books_2.bookid where authors.name = :name and books_2.title = :title_1'
    assert result == expected


def test_extension(books, model_enricher, context, ir_model_data):
    # extension on primary table, must skip the extension attribute because that's a 'reverse' foreign key
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = "insert into books(title) select :title returning books.id"
    assert result == expected


def test_extension_in_foreign_table(books, model_enricher, context, ir_model_data):
    # extension on secondary table, must join extension table to find foreign key value
    table_name = 'books'
    header = 'title[unique=true], authorid(author_id(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_authors])'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = ("insert into books(title, authorid) select :title, authors.author_id "
                "from authors left join ir_model_data on authors.author_id = ir_model_data.res_id "
                "and ir_model_data.model = 'authors' and ir_model_data.module = 'netsuite_authors' "
                "where ir_model_data.name = :name")
    assert result == expected

def test_insert_json_field(books, model_enricher):
    table_name = 'properties'
    header = 'name[unique=true], jsonb[key=en_US]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = InsertRenderer().render(mapping)
    expected = "insert into properties(name, jsonb) select :name, jsonb_build_object('en_US', :jsonb)"
    assert result == expected