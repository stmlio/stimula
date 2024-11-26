"""
This class parses an STML mapping header. It takes CSV formatted columns and outputs the corresponding mapping in as a dictionary.

Author: Romke Jonker
Email: romke@stml.io

The grammar is as follows:

attribute    : name
             | reference
             | empty

name         : ID
             | ID modifiersets

reference    : ID ( attributes )
             | ID ( attributes ) modifiersets

attributes   : attributes COLON attribute
             | attribute

modifiersets : modifiersets modifierset
             | modifierset

modifierset  : [ modifiers ]

modifiers    : modifiers COLON modifier
             | modifier

modifier     : ID = ID
             | ID = VALUE

"""
import csv
import io

from .model import Attribute, Entity, Reference
from .sly import Parser, Lexer


class StmlLexer(Lexer):
    # Set of token names.   This is always required
    tokens = {ID, VALUE, QUOTED_VALUE, COLON, EQUALS, LPAREN, RPAREN, LBRACK, RBRACK}

    # String containing ignored characters between tokens
    ignore = ' \t'

    # Regular expression rules for tokens
    ID = r'[a-zA-Z_][a-zA-Z0-9_-]*'
    VALUE = r'[a-zA-Z0-9_${}]+'
    QUOTED_VALUE = r'"[^\"]+"'
    COLON = r'\:'
    EQUALS = r'\='
    LPAREN = r'\('
    RPAREN = r'\)'
    LBRACK = r'\['
    RBRACK = r'\]'


class StmlParser(Parser):
    # CSV headers that are treated as boolean
    BOOLEAN_HEADERS = ['unique', 'skip']
    _lexer = StmlLexer()

    # Get the token list from the lexer (required)
    tokens = StmlLexer.tokens

    # main parse function that starts with decoding CSV style headers
    def parse_csv(self, table_name, header):

        # an empty string can either be an empty header or a header with a single empty column. Let's return an empty header in that case.
        if header.strip() == '':
            return Entity(table_name)

        # decode CSV style header
        csv_file = io.StringIO(header)

        # Create a CSV reader object
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"', skipinitialspace=True)

        # Parse the single line into a list of columns, we can't unescape in a single parsing step
        cells = next(csv_reader)

        # parse the cells, keep a cell counter to raise a more informative error message
        attributes = []
        for i, cell in enumerate(cells):
            try:
                attributes.append(self.parse(StmlParser._lexer.tokenize(cell)))
            except Exception as e:
                raise ValueError(f"Error parsing cell {i + 1} '{cell}': {str(e)}")

        # enable top level attributes STML editor
        for a in attributes:
            if a:
                a.enabled = True

        return Entity(table_name, attributes)

    @_('name')
    def attribute(self, p):
        return Attribute(p.name[0], **self._fix_modifier_names(p.name[1]))

    @_('reference')
    def attribute(self, p):
        return Reference(p.reference[0], p.reference[1], **self._fix_modifier_names(p.reference[2]))

    @_('')
    def attribute(self, p):
        return None

    @_('ID')
    def name(self, p):
        return (p.ID, {})

    @_('ID modifiersets')
    def name(self, p):
        return (p.ID, p.modifiersets)

    @_('ID LPAREN attributes RPAREN')
    def reference(self, p):
        return (p.ID, p.attributes, {})

    @_('ID LPAREN attributes RPAREN modifiersets')
    def reference(self, p):
        return (p.ID, p.attributes, p.modifiersets)

    @_('attributes COLON attribute')
    def attributes(self, p):
        return p.attributes + [p.attribute]

    @_('attribute')
    def attributes(self, p):
        return [p.attribute]

    @_('modifiersets modifierset')
    def modifiersets(self, p):
        return p.modifiersets | p.modifierset

    @_('modifierset')
    def modifiersets(self, p):
        return p.modifierset

    @_('LBRACK modifiers RBRACK')
    def modifierset(self, p):
        return p.modifiers

    @_('modifiers COLON modifier')
    def modifiers(self, p):
        return p.modifiers | p.modifier

    @_('modifier')
    def modifiers(self, p):
        return p.modifier

    @_('ID EQUALS ID')
    @_('ID EQUALS VALUE')
    def modifier(self, p):
        key = p[0]
        if key in self.BOOLEAN_HEADERS:
            # boolean modifier
            value = str(p[2]).lower() == 'true'
        else:
            value = p[2]

        return {key: value}

    @_('ID EQUALS QUOTED_VALUE')
    def modifier(self, p):
        key = p[0]
        # strip quotes
        value = p[2][1:-1]

        return {key: value}

    @_('')
    def empty(self, p):
        pass

    def _fix_modifier_names(self, modifiers):
        return {k.replace('-', '_'): v for k, v in modifiers.items()}

    def error(self, token):
        msg = 'Parse error'
        if token:
            # if token has value attribute
            value = getattr(token, 'value', None)
            if value:
                msg += f': encountered \'{value}\''

            # if token has index attribute
            index = getattr(token, 'index', 0)
            if index:
                msg += f' at index {index}'

        # fail fast
        raise ValueError(msg)
