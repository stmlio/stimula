import json

from stimula.header.values_parser import ValuesLexer, ValuesParser

json_object = {"menu": {
    "id": "file",
    "value": "File",
    "popup": {
        "menuitem": [
            {"value": "New", "onclick": "CreateNewDoc()"},
            {"value": "Open", "onclick": "OpenDoc()"},
            {"value": "Close", "onclick": "CloseDoc()"}
        ],
        "delay": 1.5
    }
}}

lexer = ValuesLexer()
parser = ValuesParser()


def test_json_only():
    # dump json to string using json.dumps
    text = json.dumps(json_object)

    # parse the json text
    result = parser.parse(lexer.tokenize(text))

    assert result == [json_object]


def test_double_quoted_string():
    # dump json to string using json.dumps
    text = '"abcd"'

    # parse the json text
    result = parser.parse(lexer.tokenize(text))

    assert result == ['abcd']

def _test_single_quoted_string():
    text = "'abcd'"

    # parse the text
    result = parser.parse(lexer.tokenize(text))

    assert result == ['abcd']


def test_unquoted_string():
    text = "abcd"

    # parse the text
    result = parser.parse(lexer.tokenize(text))

    assert result == [text]


def test_single_number():
    number = 1234

    # parse the text
    result = parser.parse(lexer.tokenize(str(number)))

    assert result == [number]


def test_numbers_and_json():
    # dump json to string using json.dumps
    text = f'1234:{json.dumps(json_object)}:5678'

    # parse the json text
    result = parser.parse(lexer.tokenize(text))

    assert result == [1234, json_object, 5678]
