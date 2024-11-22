from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.stml_creator import StmlCreator
from stimula.stml.stml_merger import StmlMerger
from stimula.stml.stml_parser import StmlParser


def test_merge(books, model_enricher, model_service, context):
    default_mapping = StmlCreator(model_service).create('books')

    header = 'title[unique=true], authorid(name:birthyear)'
    requested_mapping = model_enricher.enrich(StmlParser().parse_csv('books', header))

    merged_mapping = StmlMerger().merge(default_mapping, requested_mapping)

    expected = Entity('books', [
        Attribute('bookid', type='integer', primary_key=True, in_use=True),
        Attribute('title', type='text', unique=True, enabled=True, in_use=True),
        Reference('authorid', table='authors', target_name='author_id', attributes=[
            Attribute('name', type='text'),
            Attribute('birthyear', type='integer')
        ], enabled=True, in_use=True),
        Attribute('description', type='text', default=True, in_use=True),
        Attribute('price', type='numeric', default=True, in_use=True),
        Reference('seriesid', table='books', target_name='bookid', attributes=[
            Attribute('title', type='text')
        ])
    ])

    assert merged_mapping == expected


def test_merge_empty_column_in_requested_header(books, model_enricher, model_service, context):
    default_mapping = StmlCreator(model_service).create('books')

    header = 'title[unique=true],'
    requested_mapping = model_enricher.enrich(StmlParser().parse_csv('books', header))

    merged_mapping = StmlMerger().merge(default_mapping, requested_mapping)

    expected = Entity('books', [
        Attribute('bookid', type='integer', primary_key=True, in_use=True),
        Attribute('title', type='text', unique=True, enabled=True, in_use=True),
        Reference('authorid', table='authors', target_name='author_id', attributes=[
            Attribute('name', type='text')
        ], default=True, in_use=True),
        Attribute('description', type='text', default=True, in_use=True),
        Attribute('price', type='numeric', default=True, in_use=True),
        Reference('seriesid', table='books', target_name='bookid', attributes=[
            Attribute('title', type='text')
        ])
    ])

    assert merged_mapping == expected


def test_merge_primary_key_as_unique_column(books, model_enricher, model_service, context):
    # test that if a primary key is the only unique column, then the merge maintains the primary-key attribute
    table_name = 'properties'
    default_mapping = StmlCreator(model_service).create(table_name)

    header = 'property_id[unique=true], name'
    requested_mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))

    merged_mapping = StmlMerger().merge(default_mapping, requested_mapping)

    expected = Entity('properties', [
        Attribute('property_id', type='integer', primary_key=True, unique=True, enabled=True),
        Attribute('bytea', type='bytea'),
        Attribute('date', type='date'),
        Attribute('decimal', type='numeric'),
        Attribute('float', type='double precision'),
        Attribute('jsonb', type='jsonb'),
        Attribute('name', type='text', enabled=True),
        Attribute('number', type='integer'),
        Attribute('timestamp', type='timestamp'),
        Attribute('value', type='text')
    ])

    assert merged_mapping == expected


def test_merge_maps():
    # test completeness and precedence
    a = {'a': 'a', 'b': 'b'}
    b = {'a': 'A', 'c': 'c'}
    c = StmlMerger()._merge_maps(a, b)
    # acb and abc are both fine
    expected = {'a': 'A', 'c': 'c', 'b': 'b'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)


def test_merge_maps_sort_1():
    # test sorting
    a = {'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd', 'e': 'e'}
    b = {'c': 'C', 'b': 'B'}
    c = StmlMerger()._merge_maps(a, b)
    # acbde and cabde are both fine
    expected = {'a': 'a', 'c': 'C', 'b': 'B', 'd': 'd', 'e': 'e'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)


def test_merge_maps_sort_2():
    # test sorting
    a = {'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd'}
    b = {'a': 'A', 'd': 'D', 'b': 'B'}
    c = StmlMerger()._merge_maps(a, b)
    expected = {'a': 'A', 'c': 'c', 'd': 'D', 'b': 'B'}
    assert c == expected
    # also check key order
    assert list(c) == list(expected)
