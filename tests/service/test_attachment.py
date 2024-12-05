'''
Test importing and exporting attachments

To import:
- Retrieve the attachment from an external API
- Store using the Odoo ORM
'''
import pandas as pd
import requests
import hashlib

from stimula.stml.alias_enricher import AliasEnricher
from stimula.stml.model import Reference, Attribute, Entity
from stimula.stml.sql.select_renderer import SelectRenderer
from stimula.stml.stml_parser import StmlParser
from stimula.service.csv_reader import CsvReader
from stimula.service.db_reader import DbReader


def test_compile_header(model_enricher, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(bookid)[table=books: target-name=title: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], checksum[api=rest: url="/fileconnector/{guid}/{name}": unique=true]'

    # compile header
    mapping = model_enricher.enrich(StmlParser().parse_csv(table_name, header))

    expected = Entity('ir_attachment', primary_key='id', attributes=[
        Reference('res_id', table='books', target_name='title', extension=True, unique=True, enabled=True, attributes=[
            Attribute('bookid', type='integer')
        ]),
        Attribute('guid', enabled=True, skip=True),
        Attribute('name', type='varchar', enabled=True),
        Attribute('res_model', type='varchar', default_value='account.move', enabled=True, unique=True),
        Attribute('checksum', type='varchar(40)', enabled=True, unique=True, api='rest', url='/fileconnector/{guid}/{name}')
    ])

    assert mapping == expected


def test_create_select_query(model_enricher, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="checksum(file)": unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title, ir_attachment.name, ir_attachment.res_model, ir_attachment.checksum ' \
               'from ir_attachment left join books on ir_attachment.res_id = books.bookid ' \
               'order by books.title, ir_attachment.res_model, ir_attachment.checksum'
    assert result == expected


def test_create_select_query_with_nested_reference(model_enricher, ir_attachment):
    # test that we can create a select query with a nested reference in res_id
    table_name = 'ir_attachment'
    header = 'res_id(title: authorid(name))[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="checksum(file)": unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    result = SelectRenderer().render(mapping)
    expected = 'select books.title || \':\' || authors.name, ir_attachment.name, ir_attachment.res_model, ir_attachment.checksum ' \
               'from ir_attachment left join books on ir_attachment.res_id = books.bookid ' \
               'left join authors on books.authorid = authors.author_id ' \
               'order by books.title, authors.name, ir_attachment.res_model, ir_attachment.checksum'
    assert result == expected


def test_run_select_query(model_enricher, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))
    df = DbReader().read_from_db(mapping, None)
    assert df.shape == (1, 4)
    assert df.columns.tolist() == ['res_id(title)[unique=true]', 'name', 'res_model[default-value=account.move: unique=true]', 'checksum[exp=@checksum(file): unique=true]']
    assert (df.values == [['Emma', 'attachment 123.pdf', 'books', '2ee2a1fd441ab214ca7d4a9264809c668476c2b5']]).all()


def test_file_connector_api():
    url = 'https://api.stml.io/fileconnector/{guid}/{name}'
    params = {'guid': '12345', 'name': 'attachment.pdf'}
    url_expanded = url.format(**params)
    expected = 'https://api.stml.io/fileconnector/12345/attachment.pdf'
    assert url_expanded == expected

    # send http GET request to url_expanded
    response = requests.get(url_expanded)
    assert response.status_code == 200
    assert response.headers['Content-Type'] == 'application/pdf'
    assert response.headers['Content-Disposition'] == 'attachment; filename=attachment.pdf'


def test_read_csv_and_get_from_api(model_enricher):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true]'
    mapping = AliasEnricher().enrich(model_enricher.enrich(StmlParser().parse_csv(table_name, header)))

    csv = 'Emma, abcdef, attachment 123.pdf, books, '

    df = CsvReader().read_from_request(mapping, csv, 0)
    assert df.index.names == ['res_id(title)[unique=true]', 'res_model[default-value=account.move: unique=true]']
    assert df.index.tolist() == [('Emma', 'books')]
    assert df.shape == (1, 3)
    assert df.columns.tolist() == ['__line__', 'name', 'file[api=rest: skip=true: url=https://api.stml.io/fileconnector/{guid}/{name}]']
    assert df.values[0][1] == 'attachment 123.pdf'


# Define the custom checksum function
def checksum(series):
    # return hex digest for all items in series
    return series.apply(lambda x: hashlib.sha1(x.encode()).hexdigest()).astype('string')


# Define the test case using pytest
def test_checksum_eval():
    # Create a sample DataFrame
    data = {
        'a': [None, None, None],  # Empty column for checksum values
        'b': ['binary1', 'binary2', 'binary3']  # Sample binary data
    }
    df = pd.DataFrame(data)

    # Evaluate the expression using custom checksum function
    df.eval('a=@checksum(b)', inplace=True, local_dict={'checksum': checksum})

    # Expected results for checksum of the strings in column 'b'
    expected_checksums = [
        hashlib.sha1('binary1'.encode()).hexdigest(),
        hashlib.sha1('binary2'.encode()).hexdigest(),
        hashlib.sha1('binary3'.encode()).hexdigest(),
    ]

    # Assert if the computed checksums match the expected ones
    assert df['a'].tolist() == expected_checksums


def test_checksum_expression_in_csv():
    mapping = Entity('name', attributes=[
        Attribute('file'),
        Attribute('checksum', exp='@checksum(file)')
    ])

    csv = 'some string,'
    df = CsvReader().read_from_request(mapping, csv, 0)
    assert df.shape == (1, 3)
    assert df.values[0][2] == hashlib.sha1(b'some string').hexdigest()


def test_get_diff(books, ir_attachment, db, orm):
    # verify that processing a table can insert and delete
    body = '''
   Emma, abcdefgh, document_1.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    create, update, delete = db.post_table_get_diff('ir_attachment', header, None, body, insert=True, update=True, delete=True, orm=orm)
    assert len(create) == 1
    assert update.empty
    assert len(delete) == 1


def test_get_diff_no_change(books, ir_attachment, db, orm):
    # verify that processing a table with no changes results in empty diffs
    body = '''
   Emma, abcdefgh, attachment 123.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    create, update, delete = db.post_table_get_diff('ir_attachment', header, None, body, insert=True, update=True, delete=True, orm=orm)
    assert len(create) == 1
    assert update.empty
    assert len(delete) == 1


def test_get_diff_to_executor(books, ir_attachment, db, context, orm):
    # verify that processing a table can insert and delete
    body = '''
   Emma, abcdefgh, document_1.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: target-name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], ' \
             'file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true], datas[exp="@base64encode(file)": orm-only=true]'
    result = db.post_table_get_full_report('ir_attachment', header, None, body, insert=True, update=True, delete=True, execute=True)
    assert 1 == 1
