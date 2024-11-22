from stimula.stml.json_renderer import JsonRenderer
from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.stml_parser import StmlParser


def test_empty(model_enricher):
    table_name = 'books'
    header = ''
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    json = JsonRenderer().render_json(mapping)
    expected = {'table-name': 'books'}
    assert json == expected


def test_columns(books, model_enricher):
    table_name = 'books'
    header = 'title, price'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    json = JsonRenderer().render_json(mapping)
    expected = {'table-name': 'books', 'columns': [{'key': 'title', 'enabled': True, 'type': 'text'}, {'key': 'price', 'enabled': True, 'type': 'numeric'}]}
    assert json == expected


def test_modifiers(books, model_enricher):
    table_name = 'books'
    header = 'title[unique=true], price[default-value=10]'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    json = JsonRenderer().render_json(mapping)
    expected = {'table-name': 'books', 'columns': [
        {'key': 'title[unique=true]', 'unique': True, 'enabled': True, 'type': 'text'},
        {'key': 'price[default-value=10]', 'enabled': True, 'type': 'numeric'}
    ]}
    assert json == expected


def test_foreign_key(books, model_enricher):
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))
    json = JsonRenderer().render_json(mapping)
    expected = {'table-name': 'books', 'columns': [
        {'key': 'authorid(name:publisherid(publishername:country):birthyear)', 'enabled': True, 'foreign-key': True}
    ]}
    assert json == expected


def test_type(books, meta):
    mapping = Entity('authors', [
        Attribute('authorid', type='integer', primary_key=True, in_use=True),
        Attribute('name', type='text', unique=True, in_use=True, default=True),
        Attribute('birthyear', type='integer', in_use=True, default=True),
        Reference('publisherid', table='publishers', attributes=[
            Attribute('publishername', type='text'),
            Attribute('country', type='text')
        ])
    ])
    json = JsonRenderer().render_json(mapping)
    expected = {'table-name': 'authors', 'columns': [
        {'key': 'authorid', 'type': 'integer', 'in-use': True, 'primary-key': True},
        {'key': 'name[unique=true]', 'type': 'text', 'default': True, 'in-use': True, 'unique': True},
        {'key': 'birthyear', 'type': 'integer', 'default': True, 'in-use': True},
        {'key': 'publisherid(publishername:country)', 'type': 'text', 'foreign-key': True}
    ]}
    assert json == expected
