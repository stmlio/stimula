"""
This class parses an STML mapping header. It takes CSV formatted columns and outputs the corresponding mapping in as a dictionary.

Author: Romke Jonker
Email: romke@stml.io

The grammar is as follows:

cell         : columns modifiersets
             | columns
             | empty

columns      : columns : column
             | column

column       : ID ( columns )
             | ID

modifiersets : modifiersets : modifierset
             | modifierset

modifierset  : [ modifiers ]

modifiers    : modifiers : modifier
             | modifier

modifier     : ID = ID
             | ID = VALUE

"""
import csv
import io

from .header_lexer import HeaderLexer
from .sly import Parser


class StmlParser(Parser):
    # CSV headers that are treated as boolean
    BOOLEAN_HEADERS = ['unique', 'skip']
    _lexer = HeaderLexer()

    # Get the token list from the lexer (required)
    tokens = HeaderLexer.tokens

    # main parse function that starts with decoding CSV style headers
    def parse_csv(self, table_name, header):

        # an empty string can either be an empty header or a header with a single empty column. Let's return an empty header in that case.
        if header.strip() == '':
            return {'table': table_name}

        # decode CSV style header
        csv_file = io.StringIO(header)

        # Create a CSV reader object
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"', skipinitialspace=True)

        # Parse the single line into a list of cells
        cells = next(csv_reader)

        # parse the cells, keep a cell counter to raise a more informative error message
        parsed_cells = []
        for i, cell in enumerate(cells):
            try:
                parsed_cells.append(self.parse(StmlParser._lexer.tokenize(cell)))
            except Exception as e:
                raise ValueError(f"Error parsing cell {i + 1} '{cell}': {str(e)}")

        return {'table': table_name, 'columns': parsed_cells}

    @_('columns modifiersets')
    def cell(self, p):
        return {'attributes': p.columns, 'enabled': True, **p.modifiersets}

    @_('columns')
    def cell(self, p):
        return {'attributes': p.columns, 'enabled': True}

    @_('empty')
    def cell(self, p):
        return {}

    @_('columns COLON column')
    def columns(self, p):
        return p.columns + [p.column]

    @_('column')
    def columns(self, p):
        return [p.column]

    @_('ID LPAREN columns RPAREN')
    def column(self, p):
        return {'name': p.ID, 'foreign-key': {'attributes': p.columns}}

    @_('ID')
    def column(self, p):
        # column does not exist, this is fine for [skip=true] columns
        return {'name': p.ID}

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

