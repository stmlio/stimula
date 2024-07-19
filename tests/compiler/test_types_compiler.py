from stimula.compiler.types_compiler import TypesCompiler, json_to_dict, memoryview_to_string_converter
from stimula.header.csv_header_parser import HeaderParser


def test_columns(books, lexer, meta):
    # verify that compiler returns a converter to read json string as dict
    table = 'properties'
    header = 'name, jsonb'
    mapping = HeaderParser(meta, table).parse(lexer.tokenize(header))
    types = TypesCompiler().compile(mapping, header.split(', '))
    converter = types['read_csv_converters']['jsonb']
    # check converter exists
    assert converter
    # test converter with a json string
    json_str = '{"key 1": "value 1"}'
    json = converter(json_str)
    assert json == {'key 1': 'value 1'}


def test_single_quotes():
    #  test that the converter can handle json strings with single quotes, which is how they come from CSV
    json_str = "{'key 1': 'value 1'}"
    json = json_to_dict(json_str)
    assert json == {'key 1': 'value 1'}


def test_empty():
    #  test that the converter can handle an empty string
    json_str = ""
    json = json_to_dict(json_str)
    assert json is None


def test_json_with_html():
    # test that the converter can handle json strings with html
    json_str = "{'en_US': '<p class=\"o_view_nocontent_smiling_face\">\n                    Create a new document\n                </p>\n                <p>\n                    Also you will find here all the related documents and download it by clicking on any individual document.\n                </p>\n            '}"
    json = json_to_dict(json_str)
    expected = {'en_US': '''<p class="o_view_nocontent_smiling_face">
                    Create a new document
                </p>
                <p>
                    Also you will find here all the related documents and download it by clicking on any individual document.
                </p>
            '''}
    assert json == expected


def test_dtypes(books, lexer, meta):
    # verify that compiler returns a converter to read json string as dict
    table = 'properties'
    header = 'property_id, name, value, number, float, decimal, timestamp, date, jsonb'
    mapping = HeaderParser(meta, table).parse(lexer.tokenize(header))
    types = TypesCompiler().compile(mapping, header.split(', '))
    dtypes = types['read_csv_dtypes']
    expected = {'date': 'object',
                'decimal': 'float',
                'float': 'float',
                'name': 'string',
                'number': 'Int64',
                'property_id': 'Int64',
                'timestamp': 'object',
                'value': 'string'}
    assert dtypes == expected


def test_memoryview_to_string_converter():
    # test that the converter can handle memoryview objects
    mv = memoryview(b'hello')
    string = memoryview_to_string_converter(mv)
    assert string == 'hello'


def test_memoryview_to_string_converter_empty_value():
    # test that the converter can handle an empty memoryview
    string = memoryview_to_string_converter(None)
    assert string == None
