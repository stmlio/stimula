import pytest

from stimula.header.stml_parser import StmlParser


def test_empty():
    table_name = 'books'
    header = ''
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books'}
    assert result == expected


def test_columns():
    table_name = 'books'
    header = 'title, price'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title'}], 'enabled': True},
        {'attributes': [{'name': 'price'}], 'enabled': True}]}

    assert result == expected


def test_modifiers():
    table_name = 'books'
    header = 'title[unique=true], price[x=1: y=2]'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price'}], 'x': '1', 'y': '2', 'enabled': True}
    ]}
    assert result == expected


def test_modifier_sets():
    table_name = 'books'
    header = 'title[unique=true][x=1: y=2]'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title'}], 'unique': True, 'x': '1', 'y': '2', 'enabled': True},
    ]}
    assert result == expected


def test_quoted_modifiers():
    table_name = 'books'
    header = 'price[a="$=1": b="$>=2": c="$ like \'%abc%\'"]'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'price'}], 'enabled': True, 'a': '$=1', 'b': '$>=2', 'c': "$ like '%abc%'"}
    ]}
    assert result == expected


def test_modifiers_wrong_separator():
    table_name = 'books'
    header = 'title[unique=true], price[x=1, y=2]'
    with pytest.raises(ValueError, match="Parse error"):
        StmlParser().parse_csv(table_name, header)


def test_multiple_attributes():
    table_name = 'books'
    header = 'bookid:title[unique=true], price'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'bookid'}, {'name': 'title'}], 'unique': True, 'enabled': True},
        {'attributes': [{'name': 'price'}], 'enabled': True}]}
    assert result == expected


def test_foreign_key():
    table_name = 'books'
    header = 'authorid(name:publisherid(publishername:country):birthyear)'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [
            {'name': 'authorid', 'foreign-key': {'attributes': [
                {'name': 'name'},
                {'name': 'publisherid', 'foreign-key': {'attributes': [
                    {'name': 'publishername'},
                    {'name': 'country'}
                ]}},
                {'name': 'birthyear'}
            ]}}
        ], 'enabled': True}
    ]}

    assert result == expected


def test_default_value_header():
    table_name = 'books'
    header = 'title[unique=true], price[default-value=10], description[default-value="this is a book"]'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title'}], 'enabled': True, 'unique': True},
        {'attributes': [{'name': 'price'}], 'default-value': '10', 'enabled': True},
        {'attributes': [{'name': 'description'}], 'default-value': 'this is a book', 'enabled': True}
    ], }
    assert result == expected


def test_escaped_strings():
    table_name = 'books'
    header = 'title[unique=true], "price[default-value=10]", "description[default-value=""this, is a book""]"'
    result = StmlParser().parse_csv(table_name, header)
    expected = {'table': 'books', 'columns': [
        {'attributes': [{'name': 'title'}], 'enabled': True, 'unique': True},
        {'attributes': [{'name': 'price'}], 'default-value': '10', 'enabled': True},
        {'attributes': [{'name': 'description'}], 'default-value': 'this, is a book', 'enabled': True}
    ], }
    assert result == expected
