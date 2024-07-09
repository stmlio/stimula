import pandas as pd


def test_get_unicode(db, cnx, books, context):
    # test getting a string with unicode character
    text = 'this is a snake: 🐍'
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, value) VALUES (%s, %s)", ('snake', text))
        # commit
        cnx.commit()

    # get csv
    df, _ = db.get_table('properties', 'name[unique=true], value')

    dtypes = {'name[unique=true]': 'string', 'value': 'string'}
    expected = pd.DataFrame([
        ['snake', 'this is a snake: 🐍'],
    ],
        columns=['name[unique=true]', 'value']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_get_unicode_as_csv(db, cnx, books, context):
    # test getting a string with unicode character as csv
    text = 'this is a snake: 🐍'
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, value) VALUES (%s, %s)", ('snake', text))
        # commit
        cnx.commit()

    # get csv
    csv = db.get_table_as_csv('properties', 'name[unique=true], value', None)

    expected = 'name[unique=true],value\nsnake,this is a snake: 🐍\n'

    assert csv == expected


def test_update_unicode(db, cnx, books, context):
    # test that post_table without changes to a text field with unicode characters updates no rows
    text = 'this is a snake: 🐍'
    with cnx.cursor() as cr:
        # insert row into properties
        cr.execute("INSERT INTO properties (name, value) VALUES (%s, %s)", ('snake', text))
        # commit
        cnx.commit()

    body = f'''
        snake, "{text}"
    '''

    # post csv
    df = db.post_table_get_sql('properties', 'name[unique=true], value', None, body, insert=True, update=True, delete=True, execute=True)

    # assert that df has no data
    assert df.empty
