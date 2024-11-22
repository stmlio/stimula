from stimula.stml.stml_parser import StmlParser
from stimula.service.odoo.postgres_model_service import PostgresModelService


def test_get_select_statement(books, db, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'

    # parse header to build mapping
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))

    query = PostgresModelService()._create_select_query(mapping, None)

    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert query == expected
