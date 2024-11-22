from stimula.stml.model import Entity, Attribute, Reference
from stimula.stml.stml_creator import StmlCreator


def test_publishers(books, model_service, context):
    header = StmlCreator(model_service).create('publishers')
    expected = Entity('publishers', [
        Attribute('publisher_id', type='integer', primary_key=True, enabled=True),
        Attribute('country', type='text', unique=True, enabled=True),
        Attribute('publishername', type='text', unique=True, enabled=True)
    ])
    assert header == expected


def test_authors(books, model_service, context):
    header = StmlCreator(model_service).create('authors')
    expected = Entity('authors', [
        Attribute('author_id', type='integer', primary_key=True, in_use=True),
        Attribute('name', type='text', unique=True, default=True, enabled=True, in_use=True),
        Attribute('birthyear', type='integer', default=True, enabled=True, in_use=True),
        Reference('publisherid', table='publishers', target_name='publisher_id', attributes=[
            Attribute('publishername', type='text'),
            Attribute('country', type='text')
        ])
    ])
    assert header == expected


def test_books(books, model_service, context):
    header = StmlCreator(model_service).create('books')
    expected = Entity('books', [
        Attribute('bookid', type='integer', primary_key=True, in_use=True),
        Attribute('title', type='text', unique=True, default=True, enabled=True, in_use=True),
        Reference('authorid', table='authors', target_name='author_id', attributes=[
            Attribute('name', type='text')
        ], default=True, enabled=True, in_use=True),
        Attribute('description', type='text', default=True, enabled=True, in_use=True),
        Attribute('price', type='numeric', default=True, enabled=True, in_use=True),
        Reference('seriesid', table='books', target_name='bookid', attributes=[
            Attribute('title', type='text')
        ])
    ])
    assert header == expected


def test_properties_empty(books, model_service, context):
    # test all columns are enabled if the table is empty
    header = StmlCreator(model_service).create('properties')
    expected = Entity('properties', [
        Attribute('property_id', type='integer', primary_key=True, unique=True, default=True, enabled=True),
        Attribute('bytea', type='bytea', enabled=True),
        Attribute('date', type='date', enabled=True),
        Attribute('decimal', type='numeric', enabled=True),
        Attribute('float', type='double precision', enabled=True),
        Attribute('jsonb', type='jsonb', enabled=True),
        Attribute('name', type='text', enabled=True),
        Attribute('number', type='integer', enabled=True),
        Attribute('timestamp', type='timestamp', enabled=True),
        Attribute('value', type='text', enabled=True)
    ])
    assert header == expected


def test_properties_no_unique_constraints(books, model_service, context, cnx):
    # test that primary key is included if the table has no unique constraints
    with cnx.cursor() as cursor:
        cursor.execute("INSERT INTO properties(name, value) VALUES('key 1', 'value 1')")
        cnx.commit()
    header = StmlCreator(model_service).create('properties')
    expected = Entity('properties', [
        Attribute('property_id', type='integer', primary_key=True, unique=True, default=True, enabled=True, in_use=True),
        Attribute('bytea', type='bytea'),
        Attribute('date', type='date'),
        Attribute('decimal', type='numeric'),
        Attribute('float', type='double precision'),
        Attribute('jsonb', type='jsonb'),
        Attribute('name', type='text', default=True, enabled=True, in_use=True),
        Attribute('number', type='integer'),
        Attribute('timestamp', type='timestamp'),
        Attribute('value', type='text', default=True, enabled=True, in_use=True)
    ])
    assert header == expected
