"""
This class parses a text format mapping header. It connects to the database to verify the existence of tables and columns
and it resolves foreign key references.

Author: Romke Jonker
Email: romke@rnadesign.net

The grammar is as follows:

row        : cells
           | empty

cells      : cells, cell
           | cell

cell       : columns [ modifiers ]
           | columns
           | empty

columns    : columns : column
           | column

column     : ID ( columns )
           | ID

modifiers  : modifiers : modifier
           | modifier

modifier   : ID = ID
           | ID = VALUE

"""

from .header_lexer import HeaderLexer
from .sly import Parser


class HeaderParser(Parser):
    def __init__(self, metadata, table_name):
        self.metadata = metadata
        # push table name on stack
        self.table_stack = [(self._resolve_table(table_name), None)]

    # Get the token list from the lexer (required)
    tokens = HeaderLexer.tokens

    # Grammar rules and actions
    # @_('empty')
    # def row(self, p):
    #     table, _ = self.table_stack.pop()
    #     assert not self.table_stack, 'table stack should be empty after parsing'
    #     return {'table': table.name}

    @_('cells')
    def row(self, p):
        table, _ = self.table_stack.pop()
        assert not self.table_stack, 'table stack should be empty after parsing'
        cells = p.cells

        # this parser can not distinguish between an empty header and a header with a single empty column. Let's return an empty header in that case.
        if not any('attributes' in c for c in cells):
            return {'table': table.name}

        return {'table': table.name, 'columns': p.cells}

    @_('cells COMMA cell')
    def cells(self, p):
        return p.cells + [p.cell]

    @_('cell')
    def cells(self, p):
        return [p.cell]

    @_('columns LBRACK modifiers RBRACK')
    def cell(self, p):
        return {'attributes': p.columns, 'enabled': True, **p.modifiers}

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

    @_('ID push LPAREN columns RPAREN')
    def column(self, p):
        table, column = self.table_stack.pop()

        return {'name': p.ID, 'foreign-key': {'table': table.name, 'name': column, 'attributes': p.columns}}

    @_('')
    def push(self, p):
        # push target table name on stack
        column = p[-1]
        target = self._resolve_foreign_key_table(column)
        self.table_stack.append(target)

    @_('ID')
    def column(self, p):
        # verify column exists
        table, _ = self.table_stack[-1]
        if p.ID not in table.columns:
            raise ValueError(f"Column '{p.ID}' not found in table '{table.name}'")
        column = table.columns[p.ID]

        return {
            'name': str(column.key),
            'type': str(column.type).lower()
        }

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
        if key in ['unique']:
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

    def _resolve_table(self, table_name):
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found")
        return table

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

    def _resolve_foreign_key_table(self, column_name):
        # get referred column
        table, _ = self.table_stack[-1]
        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table.name}'")
        column = table.columns[column_name]
        # only know how to deal with a single foreign key per column
        if len(column.foreign_keys) != 1:
            raise ValueError(f"Expected 1 foreign key, found {len(column.foreign_keys)}")
        foreign_key = list(column.foreign_keys)[0]
        table = foreign_key.column.table
        column = foreign_key.column.name
        return table, column
