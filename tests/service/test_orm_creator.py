import pandas as pd

from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser
from stimula.service.orm_creator import InsertOrmCreator


def test_create_sql(model_compiler, books):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
    executor = InsertOrmCreator()._create_executor(1, mapping, {}, None, None)

    assert executor._query == 'select authors.author_id as authorid from authors where authors.name = :name'


def test_prepare_and_create_sql(model_compiler, books):
    table_name = 'books'
    header = 'title[unique=true], authorid(name)'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
    inserts = pd.DataFrame([
        ['Pride and Prejudice', 0, 'Jane Austen'],
    ],
        columns=['title[unique=true]', '__line__', 'authorid(name)']
    )
    executors = list(InsertOrmCreator().create_executors(mapping, inserts))

    assert executors[0]._query == 'select authors.author_id as authorid from authors where authors.name = :name'
    assert executors[0]._query_values == {'name': 'Jane Austen'}
    assert executors[0]._orm_values == {'title': 'Pride and Prejudice'}
