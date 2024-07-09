import pandas as pd


def test_get_sorted(db, books, context):
    # test that get table sorts by unique columns
    body = '''
        John Murray, England
        Thomas Egerton, England 
        Penguin Random House, England
        Penguin Random House, United States
        Penguin Random House, Spain
    '''
    # post publishers
    db.post_table_get_sql('publishers', 'publishername[unique=true], country[unique=true]', None, body, insert=True, execute=True, commit=True)

    # get publishers
    df, _ = db.get_table('publishers', 'publishername[unique=true], country[unique=true]')

    dtypes = {'publishername[unique=true]': 'string', 'country[unique=true]': 'string'}
    expected = pd.DataFrame([
        ['John Murray', 'England'],
        ['Penguin Random House', 'England'],
        ['Penguin Random House', 'Spain'],
        ['Penguin Random House', 'United States'],
        ['Thomas Egerton', 'England'],
    ],
        columns=['publishername[unique=true]', 'country[unique=true]']
    ).astype(dtypes)

    if not df.equals(expected):
        print(df.compare(expected))

    assert df.equals(expected)
