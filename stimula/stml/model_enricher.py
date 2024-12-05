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

        # process attributes, skip None columns
        [self._attribute(table, a) for a in mapping.attributes if a]

        return mapping

    def _attribute(self, table, attribute: AbstractAttribute):

        # verify column exists
        if attribute.name not in table.columns:
            # for skip and orm-only modifiers, we don't need the column to exist
            if not attribute.skip and not attribute.orm_only:
                raise ValueError(f"Column '{attribute.name}' not found in table '{table}'")
        else:
            column = table.columns[attribute.name]
            type = str(column.type).lower()
            if type == 'jsonb' and isinstance(attribute, Attribute) and attribute.key is not None:
                # represent jsonb column with key modifier as a string
                attribute.type = 'text'
            else:
                attribute.type = type

        # resolve foreign key if needed
        if isinstance(attribute, Reference):
            self._foreign_key(table, attribute)

    def _foreign_key(self, table, reference: Reference):

        # resolve foreign key table and column
        target_table, target_column_name = self._model_service.resolve_foreign_key_table(table, reference.name)

        if target_table is None:
            # foreign key not found, this is either an error condition or an extension relation, with the foreign key in the extension table.
            target_table, target_column_name = self._extension_foreign_key(reference)

            # mark the foreign key as extension
            reference.extension = True

        reference.table = target_table.name
        reference.target_name = target_column_name

        # recurse attributes
        [self._attribute(target_table, p) for p in reference.attributes]

    def _extension_foreign_key(self, reference: Reference):
        # find foreign table name in modifiers, or default to Odoo's ir_model_data table
        table_name = reference.table or 'ir_model_data'

        # resolve the table
        table = self._model_service.get_table(table_name)

        # find referred column in cell, or default to Odoo's res_id column
        column_name = reference.target_name or 'res_id'
        if column_name not in table.columns:
            raise ValueError(f"Column '{column_name}' not found in table '{table}'")

        # Odoo's ir_model_data requires a qualifier name, verify that. For other extension tables, such as ir_attachment, the qualifier is optional.
        if table_name == 'ir_model_data' and not reference.qualifier:
            raise ValueError(f"Column '{reference.name}' is an Odoo external ID column, but no 'qualifier' is specified in modifiers")

        return table, column_name
