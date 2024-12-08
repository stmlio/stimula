from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.update_renderer import UpdateRenderer
from stimula.stml.stml_parser import StmlParser


def test_simple_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set price = :price where books.title = :title'
    assert result == expected


def test_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name'
    assert result == expected


def test_multiple_join_query(books, model_enricher, context):
    # test multiple join statements in update query
    table_name = 'books'
    header = 'title[unique=true], authorid(name), seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set authorid = authors.author_id, seriesid = books_1.bookid from authors, books as books_1 where books.title = :title and authors.name = :name and books_1.title = :title_1'
    assert result == expected


def test_unique_join_query(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)[unique=true], price'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set price = :price from authors where books.title = :title and book.authorid = authors.author_id and authors.name = :name'
    assert result == expected


def test_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 where books.title = :title and books_1.title = :title_1'
    assert result == expected


def test_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_2.title = :title_1'
    assert result == expected


def test_colon_separated_columns(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set authorid = authors.author_id from authors where books.title = :title and authors.name = :name and authors.birthyear = :birthyear'
    assert result == expected


def test_compact_multiple_join_alias(books, model_enricher, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title: seriesid(title))'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set seriesid = books_1.bookid from books as books_1 left join books as books_2 on books_1.seriesid = books_2.bookid where books.title = :title and books_1.title = :title_1 and books_2.title = :title_2'
    assert result == expected


def test_unique_join_query(books, model_enricher, context):
    # test update query identifying the record by a joined column
    table_name = 'books'
    header = 'title, authorid(name)[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set title = :title from authors where books.authorid = authors.author_id and authors.name = :name'
    assert result == expected


def test_unique_multiple_join_query(books, model_enricher, context):
    # test update query identifying the record by a joined column
    table_name = 'books'
    header = 'title, seriesid(authorid(name))[unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set title = :title from books as books_1 left join authors on books_1.authorid = authors.author_id where books.seriesid = books_1.bookid and authors.name = :name'
    assert result == expected


def test_extension_not_unique(books, model_enricher, context, ir_model_data):
    # extension on primary table. If it's not marked as unique, then this query would allow updating it.
    table_name = 'books'
    header = 'title[unique=true], bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update books set bookid = ir_model_data.res_id from ir_model_data where books.title = :title and ir_model_data.name = :name and ir_model_data.module = 'netsuite_books' and ir_model_data.model = 'books'"
    assert result == expected


def test_extension_unique(books, model_enricher, context, ir_model_data):
    # extension on primary table. If it's marked as unique, then this query allows identifying the record by the extension column.
    table_name = 'books'
    header = 'title, bookid(name)[table=ir_model_data: target-name=res_id: qualifier=netsuite_books: unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update books set title = :title from ir_model_data where books.bookid = ir_model_data.res_id and ir_model_data.name = :name and ir_model_data.module = 'netsuite_books' and ir_model_data.model = 'books'"
    assert result == expected


def test_update_json_field(books, model_enricher):
    table_name = 'properties'
    header = 'name[unique=true], jsonb[key=en_US]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update properties set jsonb = jsonb_set(COALESCE(properties.jsonb, '{}'::jsonb), '{en_US}', to_jsonb(:jsonb::text)) where properties.name = :name"
    assert result == expected


def test_update_json_field_in_reference(books, model_enricher):
    # test that we can update a json field in a reference table. Odoo uses this for company specific properties.
    table_name = 'properties'
    header = 'name[unique=true], jsonb(title)[key=1: table=books: target-name=bookid]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update properties set jsonb = jsonb_set(COALESCE(properties.jsonb, '{}'::jsonb), '{1}', to_jsonb(books.bookid::text)) from books where properties.name = :name and books.title = :title"
    assert result == expected


def test_update_ambiguous_json_field(books, cnx, model_enricher):
    # test that the update query can handle a json field that appears in multiple tables
    with cnx.cursor() as cursor:
        # update properties table to add a foreign key into itself
        cursor.execute("ALTER TABLE properties ADD COLUMN propertyid INTEGER")
        cursor.execute("ALTER TABLE properties ADD CONSTRAINT fk_properties_properties FOREIGN KEY (propertyid) REFERENCES properties(property_id)")
    cnx.commit()

    table_name = 'properties'
    header = 'name[unique=true], jsonb[key=en_US], propertyid(jsonb[key=en_US])'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update properties set jsonb = jsonb_set(COALESCE(properties.jsonb, '{}'::jsonb), '{en_US}', to_jsonb(:jsonb::text)), propertyid = properties_1.property_id from properties as properties_1 where properties.name = :name and properties_1.jsonb->>'en_US' = :jsonb_1"
    assert result == expected


def test_update_where_json_field(books, model_enricher):
    table_name = 'properties'
    header = 'name, jsonb[key=en_US: unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = "update properties set name = :name where properties.jsonb->>'en_US' = :jsonb"
    assert result == expected


def test_update_with_filter_clause(books, model_enricher, context):
    # tests that UpdateRenderer can retrict un update query by a filter clause on a non-unique column.
    # it should ignore a filter on a unique attribute, because that's already restricted by the parameter
    table_name = 'books'
    header = 'title[unique=true: filter="$ like \'abc%\'"], price[filter="$ > 10"]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = UpdateRenderer().render(mapping)
    expected = 'update books set price = :price where books.title = :title and books.price > 10'
    assert result == expected
