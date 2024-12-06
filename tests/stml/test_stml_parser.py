from stimula.stml.model import Entity, Reference, Attribute
from stimula.stml.stml_parser import StmlParser


def test_empty():
    table_name = 'books'
    header = ''
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books')
    assert result == expected


def test_columns():
    table_name = 'books'
    header = 'title, price'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', enabled=True),
        Attribute('price', enabled=True)
    ])

    assert result == expected


def test_empty_columns():
    table_name = 'books'
    header = 'title, , price,, '
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', enabled=True),
        None,
        Attribute('price', enabled=True),
        None,
        None
    ])

    assert result == expected


def test_modifiers():
    table_name = 'books'
    header = 'title[unique=true], price'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True),
        Attribute('price', enabled=True)
    ])
    assert result == expected


def test_modifier_sets():
    table_name = 'books'
    header = 'title[unique=true][skip=true], price'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, skip=True, enabled=True),
        Attribute('price', enabled=True)
    ])
    assert result == expected


def test_quoted_modifiers():
    table_name = 'books'
    header = 'title[exp="$=1"], price[exp="$>=2"], price[exp="$ like \'%abc%\'"]'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', exp='$=1', enabled=True),
        Attribute('price', exp='$>=2', enabled=True),
        Attribute('price', exp="$ like \'%abc%\'", enabled=True)
    ])
    assert result == expected


def test_foreign_key():
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Reference('authorid', enabled=True, attributes=[
            Attribute('name'),
            Reference('publisherid', [
                Attribute('publishername'),
                Attribute('country')
            ]),
            Attribute('birthyear')
        ])
    ])

    assert result == expected


def test_default_value_header():
    table_name = 'books'
    header = 'title[unique=true], price[default-value=10], authorid(name[default-value="Jane Austen"])'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True),
        Attribute('price', default_value='10', enabled=True),
        Reference('authorid', enabled=True, attributes=[
            Attribute('name', default_value='Jane Austen')
        ])
    ])
    assert result == expected


def test_escaped_field():
    table_name = 'books'
    header = '"title", "price", author'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', enabled=True),
        Attribute('price', enabled=True),
        Attribute('author', enabled=True)
    ])
    assert result == expected


def test_escaped_strings():
    table_name = 'books'
    header = 'title[unique=true], "price[default-value=10]", "description[default-value=""this, is a book""]"'
    # header = 'description[default-value="this, is a book"]'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True),
        Attribute('price', default_value='10', enabled=True),
        Attribute('description', default_value='this, is a book', enabled=True)
    ])
    assert result == expected


def test_key_in_attribute():
    table_name = 'books'
    header = 'title[unique=true], price[key=nl_NL]'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True),
        Attribute('price', key='nl_NL', enabled=True)
    ])
    assert result == expected


def test_key_in_reference():
    # test that a reference can have a key, this is needed for Odoo properties that are company specific
    table_name = 'books'
    header = 'title[unique=true], authorid(name[key=nl_NL])[key=2]'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True),
        Reference('authorid', enabled=True, key='2', attributes=[
            Attribute('name', key='nl_NL')
        ])
    ])
    assert result == expected


def test_filter_src():
    # test that a reference can have a key, this is needed for Odoo properties that are company specific
    table_name = 'books'
    header = 'title[unique=true: filter-src="$"], authorid(name[key=nl_NL])[key=2: filter-src="\'a\' in $"]'
    result = StmlParser().parse_csv(table_name, header)
    expected = Entity('books', [
        Attribute('title', unique=True, enabled=True, filter_src='$'),
        Reference('authorid', enabled=True, key='2', filter_src="'a' in $", attributes=[
            Attribute('name', key='nl_NL')
        ])
    ])
    assert result == expected
