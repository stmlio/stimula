from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.sql.parameters_renderer import ParametersRenderer
from stimula.stml.stml_parser import StmlParser


def test_parameters_compiler(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = ParametersRenderer().render(mapping)
    AliasEnricher().enrich(mapping)
    expected = [('title',), ('title_1',)]
    assert result == expected


def test_parameters_compiler_with_combined_colmns(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], authorid(name:birthyear), seriesid(title)'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = ParametersRenderer().render(mapping)
    expected = [('title',), ('name', 'birthyear'), ('title_1',)]
    assert result == expected
