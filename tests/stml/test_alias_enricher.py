from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.stml_parser import StmlParser
from stimula.stml.alias_enricher import AliasEnricher


def test_columns(books, model_enricher):
    table = 'books'
    header = 'title, price'
    result = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table, header)))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', type='text', parameter='title', enabled=True),
        Attribute('price', type='numeric', parameter='price', enabled=True)
    ])
    assert result == expected


def test_modifiers(books, model_enricher):
    table = 'books'
    header = 'title[unique=true], price[exp=true]'
    result = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table, header)))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', type='text', parameter='title', unique=True, enabled=True),
        Attribute('price', type='numeric', parameter='price', exp='true', enabled=True)
    ])
    assert result == expected


def test_foreign_key(books, model_enricher):
    table = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table, header)))
    expected = Entity('books', primary_key='bookid', attributes=[
        Reference('authorid', table='authors', alias='authors', target_name='author_id', enabled=True, attributes=[
            Attribute('name', type='text', parameter='name'),
            Reference('publisherid', table='publishers', alias='publishers', target_name='publisher_id', attributes=[
                Attribute('publishername', type='text', parameter='publishername'),
                Attribute('country', type='text', parameter='country')
            ]),
            Attribute('birthyear', type='integer', parameter='birthyear')
        ])
    ])

    assert result == expected


def test_alias(books, model_enricher):
    table = 'books'
    header = 'title[unique=true], seriesid(title)'
    result = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table, header)))
    expected = Entity('books', primary_key='bookid', attributes=[
        Attribute('title', type='text', parameter='title', unique=True, enabled=True),
        Reference('seriesid', table='books', alias='books_1', target_name='bookid', enabled=True, attributes=[
            Attribute('title', type='text', parameter='title_1')
        ])
    ])
    assert result == expected
