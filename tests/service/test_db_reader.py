from stimula.header.csv_header_parser import HeaderParser
from stimula.service.db_reader import DbReader

db_reader = DbReader()


def test_get_select_statement(books, db, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'

    # parse header to build mapping
    mapping = HeaderParser(meta, table_name).parse_csv(header)

    query = db_reader._create_select_query(mapping, None)

    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert query == expected
