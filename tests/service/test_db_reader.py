from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser
from stimula.service.db_reader import DbReader

db_reader = DbReader()


def test_get_select_statement(books, db, model_compiler):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'

    # parse header to build mapping
    mapping = model_compiler.compile(StmlParser().parse_csv(table_name, header))

    query = db_reader._create_select_query(mapping, None)

    expected = 'select books.title, authors.name from books left join authors on books.authorid = authors.author_id order by books.title'
    assert query == expected
