"""
This class compiles a mapping to enrich it with model details, such as table and attribute names.

This compiler is not thread safe, because it uses a stack to keep track of the current table and column. Make sure that
each thread has its own instance of this compiler.

Author: Romke Jonker
Email: romke@rnadesign.net

"""

from stimula.service.model_service import ModelService
from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class ModelEnricher:
    # CSV headers that are treated as boolean
    BOOLEAN_HEADERS = ['unique', 'skip']

    def __init__(self, model_service: ModelService):
        self._model_service: ModelService = model_service

    def enrich(self, mapping: Entity):
        # resolve table
        table = self._model_service.get_table(mapping.name)

        # find primary key, required for ORM operations
        primary_keys = self._model_service.find_primary_keys(table)

        if len(primary_keys):
            mapping.primary_key = primary_keys[0]

        # push table name on stack
        # self.table_stack = [(table, None)]

        # process attributes, skip None columns
        [self._property(table, a) for a in mapping.attributes if a]

        # pop the stack and verify it's empty
        # table, _ = self.table_stack.pop()
        # assert not self.table_stack, 'table stack should be empty after parsing'

        return mapping

    def _property(self, table, property: AbstractAttribute):

        # verify column exists
        if property.name not in table.columns:
            # for skip and orm-only modifiers, we don't need the column to exist
            if not property.skip and not property.orm_only:
                raise ValueError(f"Column '{property.name}' not found in table '{table}'")
        else:
            column = table.columns[property.name]
            type = str(column.type).lower()
            if type == 'jsonb' and isinstance(property, Attribute) and property.key is not None:
                # represent jsonb column with key modifier as a string
                property.type = 'text'
            else:
                property.type = type

        # resolve foreign key if needed
        if isinstance(property, Reference):
            self._foreign_key(table, property)

    def _foreign_key(self, table, reference: Reference):

        # resolve foreign key table and column
        target_table, target_column_name = self._model_service.resolve_foreign_key_table(table, reference.name)

        # if table found, set foreign key table and column
        if not target_table is None:
            reference.table = target_table.name
            reference.target_name = target_column_name

            # recurse attributes
            [self._property(target_table, p) for p in reference.attributes]
        else:
            # foreign key not found, this is either an error condition or an extension relation, with the foreign key in the extension table.
            self._extension_foreign_key(reference)

    def _extension_foreign_key(self, reference: Reference):
        # find foreign table name in modifiers, or default to Odoo's ir_model_data table
        table_name = reference.table or 'ir_model_data'

        # resolve the table
        table = self._model_service.get_table(table_name)

        # find referred column in cell, or default to Odoo's res_id column
        column_name = reference.target_name or 'res_id'
        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table}'")

        reference.table = table.name
        reference.target_name = column_name

        # Odoo's ir_model_data requires a qualifier name, verify that. For other extension tables, such as ir_attachment, the qualifier is optional.
        if table_name == 'ir_model_data' and not reference.qualifier:
            raise ValueError(f"Column '{reference.name}' is an Odoo external ID column, but no 'qualifier' is specified in modifiers")

        # mark the foreign key as extension
        reference.extension = True

        # also fix attribute types
        for p in reference.attributes:
            if p.name not in table.columns:
                raise ValueError(f"Column '{p.name}' not found in table '{table}'")
            column_type = str(table.columns[p.name].type).lower()
            p.type = column_type
