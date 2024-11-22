"""
This class parses a line of text representing values and returns a list with those values.

A single position can contain a simple value, a combined value, a JSON object or a JSON array.

Author: Romke Jonker
Email: romke@rnadesign.net

Examples of permitted values:
    1. Simple values: 123, abcd
    2. Combined values: 123:abcd
    3. Double quotes: "abcd", "123", "123:abcd"
    4. Single quotes: 'abcd', '123', '123:abcd'
    5. Combined values with double quotes: "123":"abcd"
    6. Combined values with single quotes: '123':'abcd'
    7. JSON object: { "a": 1, "b": 2 }
    8. JSON array: [ 1, 2, 3, 4 ]
    9. JSON object with double quotes: "{ \"a\": 1, \"b\": 2 }"
    10. JSON array with double quotes: "[ 1, 2, 3, 4 ]"
    11. JSON object with single quotes: '{ "a": 1, "b": 2 }'
    12. JSON array with single quotes: '[ 1, 2, 3, 4 ]'
    13. Combined values with JSON: 123:{ "a": 1, "b": 2 }

"""
from .sly import Lexer, Parser
import pprint


class ValuesLexer(Lexer):
    tokens = {"FLOAT", "INTEGER", "STRING", "UNQUOTED_STRING"}

    literals = {'{', '}', '[', ']', ',', ':'}
    ignore = " \t\n"

    @_(r"[\"'].*?[\"']")
    # also accept strings in single quotes, because that's how json typically comes in a CSV file
    def STRING(self, t):
        t.value = t.value[1: -1]
        return t

    @_(r"\d+\.\d*")
    def FLOAT(self, t):
        t.value = float(t.value)
        return t

    @_(r"\d+")
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    @_(r'[^[\]{}:,"\']+')
    def UNQUOTED_STRING(self, t):
        return t


class ValuesParser(Parser):
    tokens = ValuesLexer.tokens
    start = "attributes"

    @_('attribute_value')
    def attributes(self, p):
        return [p.attribute_value]

    @_('attribute_value ":" attributes')
    def attributes(self, p):
        return [p.attribute_value] + p.attributes

    @_('value', 'UNQUOTED_STRING')
    def attribute_value(self, p):
        return p[0]

    @_('"{" members "}"')
    def object(self, p):
        return {key: value for key, value in p.members}

    @_('pair')
    def members(self, p):
        return [p.pair]

    @_('pair "," members')
    def members(self, p):
        return [p.pair] + p.members

    @_('STRING ":" value')
    def pair(self, p):
        return p.STRING, p.value

    @_('"[" elements "]"')
    def array(self, p):
        return p.elements

    @_('value')
    def elements(self, p):
        return [p.value]

    @_('value "," elements')
    def elements(self, p):
        return [p.value] + p.elements

    @_('STRING',
       'INTEGER',
       'FLOAT',
       'object',
       'array')
    def value(self, p):
        return p[0]

    def error(self, p):
        raise ValueError("Parsing error at token %s" % str(p))
