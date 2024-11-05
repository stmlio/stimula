from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.model_compiler import ModelCompiler
from stimula.compiler.parameters_compiler import ParametersCompiler

from stimula.header.stml_parser import StmlParser


def test_parameters_compiler(books, meta):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    result = ParametersCompiler().compile(mapping)
    AliasCompiler().compile(mapping)
    expected = [('title',), ('title_1',)]
    assert result == expected


def test_parameters_compiler_with_combined_colmns(books, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear), seriesid(title)'
    mapping = AliasCompiler().compile(ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header)))
    result = ParametersCompiler().compile(mapping)
    expected = [('title',), ('name', 'birthyear'), ('title_1',)]
    assert result == expected
