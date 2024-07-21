"""
This class parses a text format mapping header. It connects to the database to verify the existence of tables and columns
and it resolves foreign key references.

This parser is not thread safe, because it uses a stack to keep track of the current table and column. Make sure that
each thread has its own instance of this parser.

Author: Romke Jonker
Email: romke@rnadesign.net

The grammar is as follows:

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
import csv
import io

from .header_lexer import HeaderLexer
from .sly import Parser


class HeaderParser(Parser):
    # CSV headers that are treated as boolean
    BOOLEAN_HEADERS = ['unique', 'skip']
    _lexer = HeaderLexer()

    def __init__(self, metadata, table_name):
        self.metadata = metadata
        self._table_name = table_name

    # Get the token list from the lexer (required)
    tokens = HeaderLexer.tokens

    # main parse function that starts with decoding CSV style headers
    def parse_csv(self, header):
        # resolve table
        table = self._resolve_table(self._table_name)

        # an empty string can either be an empty header or a header with a single empty column. Let's return an empty header in that case.
        if header.strip() == '':
            return {'table': self._table_name}

        # decode CSV style header
        csv_file = io.StringIO(header)

        # Create a CSV reader object
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"', skipinitialspace=True)

        # Parse the single line into a list of cells
        cells = next(csv_reader)

        # push table name on stack
        self.table_stack = [(table, None)]

        # parse the cells
        parsed_cells = [self.parse(HeaderParser._lexer.tokenize(cell)) for cell in cells]

        # pop the stack and verify it's empty
        table, _ = self.table_stack.pop()
        assert not self.table_stack, 'table stack should be empty after parsing'

        # for extension relations, we still have to resolve the foreign key table using the modifiers
        for cell in parsed_cells:
            self._complete_extension_foreign_key(cell, cell)

        # Verify that all attributes exist in the table by checking that they have a type.
        # For columns marked as 'skip=true', the column does not have to exist in the table.
        # We can't do this check during parsing of the attribute, bec/ then the modifiers aren't known yet.
        for cell in parsed_cells:
            if not cell.get('skip'):
                for a in cell.get('attributes', []):
                    self._verify_attribute(a, table.name)

        return {'table': table.name, 'columns': parsed_cells}

    def _verify_attribute(self, attribute, table):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            for a in foreign_key.get('attributes', []):
                self._verify_attribute(a, foreign_key['table'])
        else:
            if 'type' not in attribute:
                raise ValueError(f"Column '{attribute['name']}' not found in table '{table}'")

    def _complete_extension_foreign_key(self, modifiers, cell_or_foreign_key):
        # for extension relations, we need to complete the foreign key once we have parsed the modifiers
        for attribute in cell_or_foreign_key.get('attributes', []):
            if 'foreign-key' in attribute:
                foreign_key = attribute['foreign-key']
                if 'table' in foreign_key:
                    # this is a proper foreign key, recurse to see if it has extension relations
                    self._complete_extension_foreign_key(modifiers, foreign_key)

                else:
                    # this may be an extension relation, we need to resolve the foreign key table and column

                    # find foreign table name in cell and remove the attribute
                    if 'table' not in cell_or_foreign_key:
                        raise ValueError(f"Column '{attribute['name']}' is not a foreign key and no 'table' specified in modifiers")
                    table = self._resolve_table(modifiers['table'])
                    del modifiers['table']

                    # find referred column in cell and remove the attribute
                    if 'name' not in modifiers:
                        raise ValueError(f"Column '{attribute['name']}' is not a foreign key and no referred 'name' specified in modifiers")
                    column_name = modifiers['name']
                    del modifiers['name']

                    if column_name not in table.columns:
                        raise ValueError(f"Column '{column_name}' not found in table '{table}'")
                    foreign_key['table'] = table.name
                    foreign_key['name'] = column_name

                    # Odoo's ir_model_data requires a qualifier name. Find it in cell and remove the attribute.
                    if 'qualifier' not in modifiers:
                        raise ValueError(f"Column '{attribute['name']}' is not a foreign key and no 'qualifier' specified in modifiers")
                    foreign_key['qualifier'] = modifiers['qualifier']
                    del modifiers['qualifier']

                    # mark the foreign key as extension
                    foreign_key['extension'] = True

                    # also fix attribute types
                    for a in foreign_key.get('attributes', []):
                        if a['name'] not in table.columns:
                            raise ValueError(f"Column '{a['name']}' not found in table '{table}'")
                        column_type = str(table.columns[a['name']].type).lower()
                        a['type'] = column_type

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

        if table is None:
            #     this may happen for extension relations, we need to resolve foreign table and column we have parsed the modifiers
            return {'name': p.ID, 'foreign-key': {'attributes': p.columns}}

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
        if table is None:
            # this happens for extension relations, we don't know the table until we've read the modifiers
            return {'name': p.ID}

        if p.ID not in table.columns:
            # column does not exist, this is fine for [skip=true] columns
            return {'name': p.ID}

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
            # this is either an error condition or an extension relation, with the foreign key in the extension table. We can tell once we have parsed the modifiers.
            return None, column_name

        foreign_key = list(column.foreign_keys)[0]
        table = foreign_key.column.table
        column = foreign_key.column.name
        return table, column
