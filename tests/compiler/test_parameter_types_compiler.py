from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.parameter_types_compiler import ParameterTypesCompiler

from stimula.header.stml_parser import StmlParser


def test_parameters_compiler(books, model_compiler):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
    result = ParameterTypesCompiler().compile(mapping)
    AliasCompiler().compile(mapping)
    expected = {'title': 'text', 'title_1': 'text'}
    assert result == expected


def test_parameters_compiler_with_combined_colmns(books, model_compiler):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear), seriesid(title)'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
    result = ParameterTypesCompiler().compile(mapping)
    expected = {'title': 'text', 'name': 'text', 'birthyear': 'integer', 'title_1': 'text'}
    assert result == expected
