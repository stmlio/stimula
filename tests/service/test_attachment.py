'''
Test importing and exporting attachments

To import:
- Retrieve the attachment from an external API
- Store using the Odoo ORM
'''
import pandas as pd
import requests
import hashlib

from stimula.compiler.alias_compiler import AliasCompiler
from stimula.compiler.model_compiler import ModelCompiler
from stimula.compiler.select_compiler import SelectCompiler
from stimula.header.stml_parser import StmlParser
from stimula.service.csv_reader import CsvReader
from stimula.service.db_reader import DbReader


def test_compile_header(model_compiler, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(bookid)[table=books: name=title: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], checksum[api=rest: host=$afas: url="/fileconnector/{guid}/{name}": unique=true]'

    # compile header
    mapping = model_compiler.compile(StmlParser().parse_csv(table_name, header))

    expected = {'table': 'ir_attachment', 'primary-key': 'id', 'columns': [
        {'attributes': [
            {'name': 'res_id', 'type': 'integer', 'foreign-key': {'attributes': [{'name': 'bookid', 'type': 'integer'}], 'extension': True, 'name': 'title', 'table': 'books'}}
        ], 'enabled': True, 'unique': True},
        {'attributes': [{'name': 'guid'}], 'enabled': True, 'skip': True},
        {'attributes': [{'name': 'name', 'type': 'varchar'}], 'enabled': True},
        {'attributes': [{'name': 'res_model', 'type': 'varchar'}], 'default-value': 'account.move', 'enabled': True, 'unique': True},
        {'api': 'rest', 'attributes': [{'name': 'checksum', 'type': 'varchar(40)'}], 'enabled': True, 'host': '$afas', 'unique': True, 'url': '/fileconnector/{guid}/{name}'}],
                }

    assert mapping == expected


def test_create_select_query(model_compiler, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="checksum(file)": unique=true]'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
    result = SelectCompiler().compile(mapping)
    expected = 'select books.title, ir_attachment.name, ir_attachment.res_model, ir_attachment.checksum ' \
               'from ir_attachment left join books on ir_attachment.res_id = books.bookid ' \
               'order by books.title, ir_attachment.res_model, ir_attachment.checksum'
    assert result == expected


def test_run_select_query(model_compiler, ir_attachment):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))
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


def test_read_csv_and_get_from_api(model_compiler):
    table_name = 'ir_attachment'
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true]'
    mapping = AliasCompiler().compile(model_compiler.compile(StmlParser().parse_csv(table_name, header)))

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

    # file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)"]
    # zipcode[exp="@zipcode(city)"]
    # my_color[exp="@substitute(your_color, filename)"]
    mapping = {'columns': [
        {'attributes': [{'name': 'file'}]},
        {'attributes': [{'name': 'checksum'}], 'exp': '@checksum(file)'}
    ]}

    csv = 'some string,'
    df = CsvReader().read_from_request(mapping, csv, 0)
    assert df.shape == (1, 3)
    assert df.values[0][2] == hashlib.sha1(b'some string').hexdigest()


def test_get_diff(books, ir_attachment, db, orm):
    # verify that processing a table can insert and delete
    body = '''
   Emma, abcdefgh, document_1.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    create, update, delete = db.post_table_get_diff('ir_attachment', header, None, body, insert=True, update=True, delete=True, orm=orm)
    assert len(create) == 1
    assert update.empty
    assert len(delete) == 1

def test_get_diff_no_change(books, ir_attachment, db, orm):
    # verify that processing a table with no changes results in empty diffs
    body = '''
   Emma, abcdefgh, attachment 123.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true]'
    create, update, delete = db.post_table_get_diff('ir_attachment', header, None, body, insert=True, update=True, delete=True, orm=orm)
    assert len(create) == 1
    assert update.empty
    assert len(delete) == 1

def test_get_diff_to_executor(books, ir_attachment, db, context, orm):
    # verify that processing a table can insert and delete
    body = '''
   Emma, abcdefgh, document_1.pdf, account.move,, 1234567890
    '''
    header = 'res_id(title)[table=books: name=bookid: unique=true], guid[skip=true], name, res_model[default-value="account.move": unique=true], '\
             'file[api=rest: url="https://api.stml.io/fileconnector/{guid}/{name}": skip=true], checksum[exp="@checksum(file)": unique=true], datas[exp="@base64encode(file)": orm-only=true]'
    result = db.post_table_get_full_report('ir_attachment', header, None, body, insert=True, update=True, delete=True, execute=True)
    assert 1==1