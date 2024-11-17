"""
This class compiles a mapping to enrich it with model details, such as table and attribute names.

This compiler is not thread safe, because it uses a stack to keep track of the current table and column. Make sure that
each thread has its own instance of this compiler.

Author: Romke Jonker
Email: romke@rnadesign.net

"""

from stimula.service.model_service import ModelService


class ModelCompiler:
    # CSV headers that are treated as boolean
    BOOLEAN_HEADERS = ['unique', 'skip']

    def __init__(self, model_service: ModelService):
        self._model_service: ModelService = model_service

    def compile(self, mapping):
        # resolve table
        table = self._model_service.get_table(mapping['table'])

        # find primary key, required for ORM operations
        primary_keys = self._model_service.find_primary_keys(table)

        if len(primary_keys):
            mapping['primary-key'] = primary_keys[0]

        # push table name on stack
        self.table_stack = [(table, None)]

        # process columns
        [self._column(table, column) for column in mapping.get('columns', [])]

        # pop the stack and verify it's empty
        table, _ = self.table_stack.pop()
        assert not self.table_stack, 'table stack should be empty after parsing'

        return mapping

    def _column(self, table, column):
        # get modifiers, todo: move to modifiers dict
        modifiers = column

        # process attribute lists
        [self._attribute(table, attribute, modifiers) for attribute in column.get('attributes', [])]

        # self._complete_extension_foreign_key(cell, cell)

    def _attribute(self, table, attribute, modifiers):
        # verify column exists
        if attribute['name'] not in table.columns:
            # for skip and orm-only modifiers, we don't need the column to exist
            if not modifiers.get('skip') and not modifiers.get('orm-only'):
                raise ValueError(f"Column '{attribute['name']}' not found in table '{table}'")
        else:
            column = table.columns[attribute['name']]
            type = str(column.type).lower()
            if type == 'jsonb' and 'key' in modifiers:
                # represent jsonb column with key modifier as a string
                attribute['type'] = 'text'
                # also mark this as a jsonb column. This is needed until we have nested modifiers
                attribute['jsonb'] = True
            else:
                attribute['type'] = type

        # resolve foreign key if needed
        if 'foreign-key' in attribute:
            self._foreign_key(table, attribute, modifiers)

    def _foreign_key(self, table, attribute, modifiers):
        # get foreign key object
        foreign_key = attribute['foreign-key']

        # resolve foriegn key table and column
        target_table, target_column_name = self._model_service.resolve_foreign_key_table(table, attribute['name'])

        # if table found, set foreign key table and column
        if not target_table is None:
            foreign_key['table'] = target_table.name
            foreign_key['name'] = target_column_name

            # recurse attributes
            [self._attribute(target_table, a, modifiers) for a in foreign_key.get('attributes', [])]
        else:
            # foreign key not found, this is either an error condition or an extension relation, with the foreign key in the extension table.
            self._extension_foreign_key(attribute, modifiers)

    def _extension_foreign_key(self, attribute, modifiers):
        # get foreign key object
        foreign_key = attribute['foreign-key']

        # find foreign table name in modifiers, or default to Odoo's ir_model_data table
        table_name = modifiers.get('table', 'ir_model_data')
        # resolve the table
        table = self._model_service.get_table(table_name)
        # remove the attribute because we don't need it as a modifier
        if 'table' in modifiers:
            del modifiers['table']

        # find referred column in cell, or default to Odoo's res_id column
        column_name = modifiers.get('name', 'res_id')
        # remove the attribute because we don't need it as a modifier
        if 'name' in modifiers:
            del modifiers['name']

        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table}'")
        foreign_key['table'] = table.name
        foreign_key['name'] = column_name

        # Odoo's ir_model_data requires a qualifier name, verify that. For other extension tables, such as ir_attachment, the qualifier is optional.
        if table_name == 'ir_model_data' and 'qualifier' not in modifiers:
            raise ValueError(f"Column '{attribute['name']}' is an Odoo external ID column, but no 'qualifier' is specified in modifiers")

        # Find the qualifier in cell and remove the attribute.
        if 'qualifier' in modifiers:
            foreign_key['qualifier'] = modifiers['qualifier']
            del modifiers['qualifier']

        # there may be an optional id attribute in the cell as well, move it to the attribute
        if 'id' in modifiers:
            foreign_key['id'] = modifiers['id']
            del modifiers['id']

        # mark the foreign key as extension
        foreign_key['extension'] = True

        # also fix attribute types
        for a in foreign_key.get('attributes', []):
            if a['name'] not in table.columns:
                raise ValueError(f"Column '{a['name']}' not found in table '{table}'")
            column_type = str(table.columns[a['name']].type).lower()
            a['type'] = column_type
