import json
from datetime import datetime


def test_update_number(db, cnx, books, context):
    # test that post_table without changes to a number field updates no rows
    number = 1234
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, number) VALUES (%s, %s)", ('key 0', number))
        # commit
        cnx.commit()

    body = f'''
        key 0, {number}
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], number', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_number_when_containing_null(db, cnx, books, context):
    # test that post_table without changes to a number field updates no rows, even when the number field contains a null value
    number = 1234
    with cnx.cursor() as cr:
        # insert rows into properties
        cr.execute("INSERT INTO properties (name, number) VALUES (%s, %s)", ('key 0', number))
        cr.execute("INSERT INTO properties (name) VALUES (%s)", ('key 1',))
        # commit
        cnx.commit()

    body = f'''
        key 0, {number}
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], number', None, body, insert=True, update=True, delete=False, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_foreign_key(db, cnx, books, context):
    # test that post_table without changes to a foreign key field updates no rows

    body = f'''
        Emma, 1
    '''

    # post csv
    df = db.post_table_get_sql('books', 'title[unique=true], authorid(author_id)', None, body, insert=True, update=True, delete=False, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_jsonb(db, cnx, books, context):
    # test that post_table without changes to a jsonb field updates no rows
    jsonb_data = {'key 1': 'value 1'}
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, jsonb) VALUES (%s, %s)", ('key 1', json.dumps(jsonb_data)))
        # commit
        cnx.commit()

    body = f'''
        key 1, {json.dumps(jsonb_data)}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], jsonb', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_timestamp(db, cnx, books, context):
    # test that post_table without changes to a timestamp field updates no rows
    time_string = '2020-01-31 23:24:25.123456'
    # convert to datetime
    timestamp = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, timestamp) VALUES (%s, %s)", ('key 2', timestamp))
        # commit
        cnx.commit()

    body = f'''
        key 2, {timestamp}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], timestamp', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_timestamp_and_number(db, cnx, books, context):
    # test that post_table without changes to a timestamp and a number field updates no rows
    time_string = '2020-01-31 23:24:25.123456'
    # convert to datetime
    timestamp = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
    number = 1234
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, timestamp, number) VALUES (%s, %s, %s)", ('key 2', timestamp, number))
        # commit
        cnx.commit()

    body = f'''
        key 2, {timestamp}, {number}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], timestamp, number', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_number_and_timestamp(db, cnx, books, context):
    # test that post_table without changes to a timestamp and a number field updates no rows
    # same test but in different order, to trace an issue with datetime import
    time_string = '2020-01-31 23:24:25.123456'
    # convert to datetime
    timestamp = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
    number = 1234
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, timestamp, number) VALUES (%s, %s, %s)", ('key 2', timestamp, number))
        # commit
        cnx.commit()

    body = f'''
        key 2, {number}, {timestamp}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], number, timestamp', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_date(db, cnx, books, context):
    # test that post_table without changes to a timestamp field updates no rows
    date_string = '2020-01-31'
    # convert to date
    date = datetime.strptime(date_string, '%Y-%m-%d').date()
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, date) VALUES (%s, %s)", ('key 3', date))
        # commit
        cnx.commit()

    body = f'''
        key 3, {date}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], date', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_date_as_unique_key(db, cnx, books, context):
    # test that post_table without changes to a date field updates no rows, even if the date field is a unique key
    date_string = '2020-01-31'
    # convert to date
    date = datetime.strptime(date_string, '%Y-%m-%d').date()
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, date) VALUES (%s, %s)", ('key 3', date))
        # commit
        cnx.commit()

    body = f'''
        key 3, {date}
    '''

    # post jsonb
    df = db.post_table_get_sql('properties', 'name[unique=true], date[unique=true]', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_get_bytea(db, cnx, books, context):
    bytea = b'\x00\x01\x02\x03\x04'
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, bytea) VALUES (%s, %s)", ('key 0', bytea))
        # commit
        cnx.commit()

    # get table
    csv = db.get_table_as_csv('properties', 'name, bytea', None, escapechar='\\')

    expected = 'name,bytea\nkey 0,\x00\x01\x02\x03\x04\n'

    assert csv == expected


def test_insert_unicode_binary(db, cnx, books, context):
    # test that post_table can insert a mixed binary string
    body = f'''
        key 0, "abcd\\xf0\\x9f\\x90\\x8d\\x41\\x42\\x43\\x44"
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], bytea', None, body, insert=True, update=True, delete=True, execute=True, commit=True)

    # assert that df has no data
    assert df.values[0][3] == 'abcd🐍ABCD'


def test_insert_latin_1_binary(db, cnx, books, context):
    # test that post_table can insert a mixed binary string
    body = f'''
        key 0, "abcd\\xa0\\x41\\x42\\x43\\x44"
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], bytea', None, body, insert=True, update=True, delete=True, execute=True, commit=True)

    # assert that df has no data
    assert df.values[0][3] == 'abcd\xa0ABCD'


def test_update_bytea(db, cnx, books, context):
    # test that post_table without changes to a binary string field updates no rows
    bytea = b'abcd\xa0ABCD'
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, bytea) VALUES (%s, %s)", ('key 0', bytea))
        # commit
        cnx.commit()

    body = f'''
        key 0, "abcd\\xa0\\x41\\x42\\x43\\x44"
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], bytea', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty


def test_update_empty_text(db, cnx, books, context):
    # test that post_table without changes to an empty text field updates no rows
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, value) VALUES (%s, %s)", ('key 0', ''))
        # commit
        cnx.commit()

    body = f'''
        key 0,
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], value', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty
