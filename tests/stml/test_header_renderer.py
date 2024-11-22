from stimula.stml.header_renderer import HeaderRenderer
from stimula.stml.stml_parser import StmlParser


def test_empty(model_enricher):
    table_name = 'books'
    header = ''
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv(mapping)
    assert csv == header


def test_columns(books, model_enricher):
    table_name = 'books'
    header = 'title, price'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv(mapping)
    assert csv == header


def test_modifiers(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], price[default-value=10]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv(mapping)
    assert csv == header


def test_modifier_with_quoted_value(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], price[filter="$ = \'abc\'"]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv(mapping)
    assert csv == header


def test_foreign_key(books, model_enricher):
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv(mapping)
    assert csv == header


def test_list(books, model_enricher):
    table_name = 'books'
    header = 'title, price'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    list = HeaderRenderer().render_list(mapping)
    assert list == ['title', 'price']


def test_unique_columns(books, model_enricher):
    table_name = 'books'
    header = 'authorid( name : birthyear) [unique =true], price  '
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    csv = HeaderRenderer().render_csv_unique(mapping)
    assert csv == 'authorid(name:birthyear)[unique=true]'


def test_unique_list(books, model_enricher):
    table_name = 'books'
    header = 'authorid( name : birthyear) [unique =true], price  '
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    list = HeaderRenderer().render_list_unique(mapping)
    assert list == ['authorid(name:birthyear)[unique=true]']


def test_non_unique_list(books, model_enricher):
    table_name = 'books'
    header = 'authorid( name : birthyear) [unique =true], price  '
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    list = HeaderRenderer().render_list_non_unique(mapping)
    assert list == ['price']


def test_list_with_skip(books, model_enricher):
    table_name = 'books'
    header = ' title [unique =true], price, xyz[skip=true]  '
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    list = HeaderRenderer().render_list(mapping)
    assert list == ['title[unique=true]', 'price']
