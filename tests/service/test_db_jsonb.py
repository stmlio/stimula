import json

import pandas as pd

from stimula.compiler.model_compiler import ModelCompiler
from stimula.header.stml_parser import StmlParser


def test_get_jsonb(db, cnx, books, context):
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    # get table
    df, _ = db.get_table('properties', 'name, jsonb')

    # expected data types, jsonb is stored as object
    dtypes = {'name': 'string', 'jsonb': 'object'}
    expected = pd.DataFrame([
        ['key 1', jsonb_data],
    ],
        columns=['name', 'jsonb']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_post_jsonb(db, cnx, books, context):
    jsonb_data = {'key 1': 'value 1'}

    body = f'''
        key 1, {json.dumps(jsonb_data)}
    '''

    # post jsonb
    db.post_table_get_sql('properties', 'name[unique=true], jsonb', None, body, insert=True, execute=True)

    with cnx.cursor() as cr:
        # select from properties
        cr.execute("select jsonb from properties")

        # fetch all rows
        rows = cr.fetchall()

        # verify jsonb data
        assert rows[0][0] == jsonb_data


def test_get_jsonb_in_combined_column(db, cnx, books, context):
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    # get table
    df, _ = db.get_table('properties', 'name:jsonb')

    # expected data types
    dtypes = {'name:jsonb': 'string'}
    expected = pd.DataFrame([
        [f'key 1:{json.dumps(jsonb_data)}'],
    ],
        columns=['name:jsonb']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_get_jsonb_in_combined_column_reversed(db, cnx, books, context):
    # test that get table works with jsonb in combined column, the json field coming first
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    # get table
    df, _ = db.get_table('properties', 'jsonb:name')

    # expected data types
    dtypes = {'jsonb:name': 'string'}
    expected = pd.DataFrame([
        [f'{json.dumps(jsonb_data)}:key 1'],
    ],
        columns=['jsonb:name']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_get_jsonb_in_combined_unique_column(db, cnx, books, context):
    # test that get table works with jsonb in combined column that is also a unique column
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    # get table
    df, _ = db.get_table('properties', 'jsonb:name[unique=true]')

    # expected data types
    dtypes = {'jsonb:name[unique=true]': 'string'}
    expected = pd.DataFrame([
        [f'{json.dumps(jsonb_data)}:key 1'],
    ],
        columns=['jsonb:name[unique=true]']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_post_jsonb_in_combined_column(db, cnx, books, context):
    jsonb_data = {'key 1': 'value 1'}
    # replace double quotes with single quotes
    json_string = json.dumps(jsonb_data).replace('"', "'")

    body = f'''
        key 1, 123:{json_string}
    '''

    # post jsonb
    db.post_table_get_sql('properties', 'name[unique=true], number:jsonb', None, body, insert=True, execute=True)

    with cnx.cursor() as cr:
        # select from properties
        cr.execute("select jsonb from properties")

        # fetch all rows
        rows = cr.fetchall()

        # verify jsonb data
        assert rows[0][0] == jsonb_data


def test_get_jsonb_with_unicode_as_csv(db, cnx, books, context):
    # test getting a jsonb with unicode character that is represented as the original, and one that is escaped
    jsonb_data = {'key 1': 'value 1', 'snake': '🐍', 'nbsp': ' '}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    # get table
    csv = db.get_table_as_csv('properties', 'name, jsonb', None)

    expected = 'name,jsonb\nkey 1,"{""nbsp"": ""\xa0"", ""key 1"": ""value 1"", ""snake"": ""🐍""}"\n'

    assert csv == expected


def _test_post_jsonb_with_unicode(db, cnx, books, context):
    # need to decide if we support '\xa0' like escapes in csv
    jsonb_data = {'key 1': 'value 1', 'snake': '🐍', 'nbsp': ' ', 'escaped': '\xa0'}

    json_text = "{'key 1': 'value 1', 'snake': '🐍', 'nbsp': ' ', 'escaped': '\\xa0'}"
    body = f'''
        key 1, "{json_text}"
    '''

    # post jsonb
    db.post_table_get_sql('properties', 'name[unique=true], jsonb', None, body, insert=True, execute=True, commit=True)

    with cnx.cursor() as cr:
        # select from properties
        cr.execute("select jsonb from properties")

        # fetch all rows
        rows = cr.fetchall()

        # verify jsonb data
        assert rows[0][0] == jsonb_data


def test_conversion_of_json_with_null_value(db, meta):
    table_name = 'properties'
    header = 'name, jsonb'
    mapping = ModelCompiler(meta).compile(StmlParser().parse_csv(table_name, header))

    # test that DB can correctly convert a df with a json that has a null value
    df = pd.DataFrame([
        [1, {"key": None}],
    ],
        columns=['name', 'jsonb']
    )

    # convert to csv
    csv = db.convert_to_csv(df, mapping)

    expected = "name,jsonb\n1,\"{\"\"key\"\": null}\"\n"

    assert csv == expected


def _test_post_json_object_as_unique_key(db, meta):
    # test that DB can post a json object as a unique key, this requires the resulting dict to be immutable
    # disabled for now, because the current implementation does not support json as unique key
    header = 'name, jsonb[unique=true]'
    jsonb_data = {'key 1': 'value 1'}

    body = f'''
        key 1, {json.dumps(jsonb_data)}
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    dtypes = {'jsonb:name[unique=true]': 'string'}
    expected = pd.DataFrame([
        [f'{json.dumps(jsonb_data)}:key 1'],
    ],
        columns=['jsonb:name[unique=true]']
    ).astype(dtypes)

    assert result.equals(expected)


def test_post_json_from_string(db, meta, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier
    header = 'name[unique=true], jsonb[key=en_US]'

    body = f'''
        key 2, A string to convert to JSON
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    assert result['jsonb'][0].adapted == {"en_US": "A string to convert to JSON"}


def test_post_json_from_string_unique(db, meta, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier, even when it's a unique column
    header = 'name, jsonb[key=en_US: unique=true]'

    body = f'''
        key 2, A string to convert to JSON
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    assert result['jsonb'][0].adapted == {"en_US": "A string to convert to JSON"}
