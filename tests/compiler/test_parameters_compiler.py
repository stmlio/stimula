from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.parameters_compiler import ParametersCompiler
from stimula.header.csv_header_parser import HeaderParser


def test_parameters_compiler(books, lexer, meta):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table_name).parse_csv(header))
    result = ParametersCompiler().compile(mapping)
    AliasCompiler().compile(mapping)
    expected = [('title',), ('title_1',)]
    assert result == expected


def test_parameters_compiler_with_combined_colmns(books, lexer, meta):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear), seriesid(title)'
    mapping = AliasCompiler().compile(HeaderParser(meta, table_name).parse_csv(header))
    result = ParametersCompiler().compile(mapping)
    expected = [('title',), ('name', 'birthyear'), ('title_1',)]
    assert result == expected
