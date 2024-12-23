import json

import pandas as pd

from stimula.stml.stml_parser import StmlParser


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


def test_get_jsonb_in_combined_column(db, cnx, properties_relation, context):
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cursor:
        # insert row into properties
        cursor.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        cursor.execute("update books set propertyid = %s where books.title = %s", (1, 'Emma'))
        # commit
        cnx.commit()

    # get table
    header = 'title, propertyid(name:jsonb)'
    df, _ = db.get_table('books', header)

    # get row with title 'Emma'
    df = df[df['title'] == 'Emma']

    # get value from second column
    value = df['propertyid(name:jsonb)'].values[0]

    expected = f'key 1:{json.dumps(jsonb_data)}'

    assert value == expected


def test_get_jsonb_in_combined_column_reversed(db, cnx, properties_relation, context):
    # test that get table works with jsonb in combined column, the json field coming first
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cursor:
        # insert row into properties
        cursor.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        cursor.execute("update books set propertyid = %s where books.title = %s", (1, 'Emma'))
        # commit
        cnx.commit()

    # get table
    header = 'title, propertyid(jsonb:name)'
    df, _ = db.get_table('books', header)

    # get row with title 'Emma'
    df = df[df['title'] == 'Emma']

    # get value from second column
    value = df['propertyid(jsonb:name)'].values[0]

    expected = f'{json.dumps(jsonb_data)}:key 1'

    assert value == expected


def test_get_jsonb_in_combined_unique_column(db, cnx, properties_relation, context):
    # test that get table works with jsonb in combined column that is also a unique column
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cursor:
        # insert row into properties
        cursor.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        cursor.execute("update books set propertyid = %s where books.title = %s", (1, 'Emma'))
        # commit
        cnx.commit()

    # get table
    header = 'title, propertyid(jsonb:name)[unique=true]'
    df, _ = db.get_table('books', header)

    # get row with title 'Emma'
    df = df[df['title'] == 'Emma']

    # get value from second column
    value = df['propertyid(jsonb:name)[unique=true]'].values[0]

    expected = f'{json.dumps(jsonb_data)}:key 1'

    assert value == expected




def test_get_jsonb_with_unicode_as_csv(db, cnx, books, context):
    # test getting a jsonb with unicode character that is represented as the original, and one that is escaped
    jsonb_data = {'key 1': 'value 1', 'snake': '🐍', 'nbsp': ' '}
    with cnx.cursor() as cursor:
        # insert row into properties
        cursor.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
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


def test_conversion_of_json_with_null_value(db, model_enricher):
    table_name = 'properties'
    header = 'name, jsonb'
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))

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


def _test_post_json_object_as_unique_key(db, model_enricher):
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


def test_post_json_from_string(cnx, db, model_enricher, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier
    header = 'name[unique=true], jsonb[key=en_US]'

    body = f'''
        key 2, A string to convert to JSON
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    # value should not be converted to json
    assert result['rows'][0] == 1
    assert result['jsonb'][0] == "A string to convert to JSON"

    with cnx.cursor() as cr:
        cr.execute("select jsonb from properties where name = 'key 2'")
        rows = cr.fetchall()

        # verify jsonb data
        assert rows[0][0] == {"en_US": "A string to convert to JSON"}


def test_post_json_from_string_unique(books, db, model_enricher, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier, even when it's a unique column
    header = 'name, jsonb[key=en_US: unique=true]'

    body = f'''
        key 2, A string to convert to JSON
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    assert result['rows'][0] == 1
    assert result['jsonb'][0] == "A string to convert to JSON"


def test_post_json_from_string_in_multi_index(db, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier, even when it's a unique column
    header = 'name[unique=true], jsonb[key=en_US: unique=true]'

    body = f'''
        key 3, A string to convert to JSON
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    assert result['rows'][0] == 1
    assert result['jsonb'][0] == "A string to convert to JSON"


def test_json_in_foreign_key(db, context, properties_relation, cnx):
    # test that DB can use json in a foreign key relationship
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    header = 'title[unique=true], authorid(name), propertyid(jsonb[key="key 1"])'

    body = f'''
        title 1, Jane Austen, value 1
    '''

    # post jsonb
    result = db.post_table_get_sql('books', header, None, body, insert=True, execute=True)

    assert result['rows'][0] == 1
    assert result['jsonb'][0] == "value 1"


def test_json_default_value(db, context, cnx):
    # test that DB can combine jsonb with a default value
    header = 'name[unique=true], jsonb[key="key 1": default-value="value 1"]'

    body = f'''
        key 2, 
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, insert=True, execute=True)

    assert result['jsonb'][0] == "value 1"


def test_update_json(books, db, context, cnx):
    # test that DB can update json
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cursor:
        # insert row into properties
        cursor.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('name 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    header = 'name[unique=true], jsonb[key="key 1"]'

    body = f'''
        name 1, value 2
    '''

    # post jsonb
    result = db.post_table_get_sql('properties', header, None, body, update=True, execute=True)

    expected = "update properties set jsonb = jsonb_set(COALESCE(properties.jsonb, '{}'::jsonb), '{key 1}', to_jsonb(:jsonb::text)) where properties.name = :name"

    assert (result.values[0] == [1, None, expected, 'name 1', 'value 2']).all()


def test_post_json_set_two_keys(cnx, books, db, model_enricher, context):
    # test that DB can convert a string into a json object, based on the 'key' modifier
    header1 = 'name[unique=true], jsonb[key="key 1"]'

    body1 = f'''
        name 1, value 1
    '''

    # post jsonb
    db.post_table_get_sql('properties', header1, None, body1, insert=True, execute=True, commit=True)

    header2 = 'name[unique=true], jsonb[key="key 2"]'

    body2 = f'''
        name 1, value 2
    '''
    db.post_table_get_sql('properties', header2, None, body2, update=True, execute=True, commit=True)

    with cnx.cursor() as cr:
        cr.execute("select jsonb from properties where name = 'name 1'")
        rows = cr.fetchall()

        # verify jsonb data
        assert rows[0][0] == {"key 1": "value 1", "key 2": "value 2"}
