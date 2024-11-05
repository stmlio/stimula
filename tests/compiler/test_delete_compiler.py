from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.delete_compiler import DeleteCompiler

from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser


def test_simple_query(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], price'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books where books.title = :title'
    assert result == expected


def test_multiple_unique_columns(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], price[unique=true]'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books where books.title = :title and books.price = :price'
    assert result == expected


def test_combined_unique_columns(books, meta, context):
    table_name = 'books'
    header = 'title:price[unique=true]'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books where books.title = :title and books.price = :price'
    assert result == expected


def test_with_joins(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], price, authorid(name)'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books where books.title = :title'
    assert result == expected


def test_delete_by_foreign_key(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], price, authorid(publisherid(publishername))[unique=true]'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books using authors left join publishers on authors.publisherid = publishers.publisher_id where books.title = :title and books.authorid = authors.author_id and publishers.publishername = :publishername'
    assert result == expected


def test_delete_by_composed_foreign_keys(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], price, authorid(name:publisherid(publishername:country))[unique=true]'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books using authors left join publishers on authors.publisherid = publishers.publisher_id where books.title = :title and books.authorid = authors.author_id and authors.name = :name and publishers.publishername = :publishername and publishers.country = :country'
    assert result == expected


def test_delete_by_double_join(books, meta, context):
    table_name = 'books'
    header = 'authorid(publisherid(publishername):publisherid(country))[unique=true]'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books using authors left join publishers on authors.publisherid = publishers.publisher_id left join publishers as publishers_1 on authors.publisherid = publishers_1.publisher_id where books.authorid = authors.author_id and publishers.publishername = :publishername and publishers_1.country = :country'
    assert result == expected


def test_delete_by_self_join(books, meta, context):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)[unique=true]'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    result = DeleteCompiler().compile(mapping)
    expected = 'delete from books using books as books_1 where books.title = :title and books.seriesid = books_1.bookid and books_1.title = :title_1'
    assert result == expected
