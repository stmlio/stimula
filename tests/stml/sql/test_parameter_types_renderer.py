from stimula.stml.stml_parser import StmlParser
from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.parameter_types_renderer import ParameterTypesRenderer


def test_parameters_compiler(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = ParameterTypesRenderer().render(mapping)
    AliasEnricher().enrich(mapping)
    expected = {'title': 'text', 'title_1': 'text'}
    assert result == expected


def test_parameters_compiler_with_combined_colmns(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear), seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = ParameterTypesRenderer().render(mapping)
    expected = {'title': 'text', 'name': 'text', 'birthyear': 'integer', 'title_1': 'text'}
    assert result == expected
