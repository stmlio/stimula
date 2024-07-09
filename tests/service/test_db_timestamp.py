import numpy as np
import pandas as pd


def test_post_timestamp(db, books, context):
    # test that posting a timestamp to a table with a timestamp column sets the timestamp
    body = '''
        key 1, value 1, 2021-02-03 10:11:12
    '''
    df = db.post_table_get_sql('properties', 'name[unique=true], value, timestamp', None, body, insert=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'name': 'string', 'value': 'string', 'timestamp': np.dtype('datetime64[ns]')}
    expected = pd.DataFrame([
        [1, 'insert into properties(name, value, timestamp) select :name, :value, :timestamp', 'key 1', 'value 1', '2021-02-03 10:11:12']
    ],
        columns=['rows', 'sql', 'name', 'value', 'timestamp']
    ).astype(dtypes)

    assert df.equals(expected)


def test_get_timestamp(db, books, context):
    # test that posting a timestamp to a table with a timestamp column sets the timestamp
    body = '''
        key 1, value 1, 2021-02-03 10:11:12
    '''
    db.post_table_get_sql('properties', 'name[unique=true], value, timestamp', None, body, insert=True, execute=True, commit=True)

    df, _ = db.get_table('properties', 'timestamp')

    dtypes = {'timestamp': np.datetime64()}
    expected = pd.DataFrame([
        [pd.Timestamp('2021-02-03 10:11:12')],
    ],
        columns=['timestamp']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)


def test_update_timestamp(db, books, context):
    # test that posting a timestamp to a table with a timestamp column sets the timestamp
    body1 = '''
        key 1, value 1, 2021-02-03 10:11:12
    '''
    db.post_table_get_sql('properties', 'name[unique=true], value, timestamp', None, body1, insert=True, execute=True, commit=True)

    body2 = '''
        key 1, value 1, 2021-02-03 10:11:13
    '''
    df = db.post_table_get_sql('properties', 'name[unique=true], value, timestamp', None, body2, update=True, execute=True)

    dtypes = {'rows': pd.Int64Dtype(), 'sql': 'string', 'name': 'string', 'timestamp': np.dtype('datetime64[ns]')}
    expected = pd.DataFrame([
        [1, 'update properties set timestamp = :timestamp where properties.name = :name', 'key 1', '2021-02-03 10:11:13']
    ],
        columns=['rows', 'sql', 'name', 'timestamp']
    ).astype(dtypes)

    assert df.equals(expected)
