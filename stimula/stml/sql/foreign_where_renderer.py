'''
Compiles foreign key where clauses for insert and update.
This compiler is used to filter the foreign key tables when inserting or updating records to obtain the foreign key values.
'''

from itertools import chain
from typing import List

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class ForeignWhereClauseRenderer:

    def __init__(self, is_insert_query, is_delete_query):
        # if this is an insert query, we must not join with extension tables
        self._is_insert_query = is_insert_query
        self._is_delete_query = is_delete_query
        self._aliases = {}

    def render(self, mapping: Entity) -> str:
        # no need to list non-unique columns as 'using' table when creating a delete query
        clauses = list(chain(*[self._attribute(a, mapping.name) for a in mapping.attributes if a.unique or not self._is_delete_query]))

        return ' and '.join(clauses)

    def _attribute(self, attribute: AbstractAttribute, alias) -> List[str]:
        if isinstance(attribute, Attribute):
            # no where-clause needed for root table. Only add where clauses for joined tables.
            return []

        if isinstance(attribute, Reference):

            # don't add extension conditions if this is an insert query
            if attribute.extension and self._is_insert_query:
                return []

            # recurse
            target_alias = attribute.alias or attribute.table
            where_clause = self._nested_attributes(attribute.attributes, target_alias)

            # add extension conditions if this is the root of an update or delete query
            if attribute.extension and attribute.qualifier and not self._is_insert_query:
                # assume for now that the alias is the table name. This is fine as long as we're not joining the same table multiple times
                model = alias.replace('_', '.')
                extension_clause = [f"{target_alias}.module = '{attribute.qualifier}'",
                                    f"{target_alias}.model = '{model}'"]
                where_clause = chain(where_clause, extension_clause)

            return where_clause

    def _nested_attribute(self, attribute: AbstractAttribute, alias):
        if isinstance(attribute, Attribute):

            # terminate foreign key with an equation
            parameter_name = attribute.parameter or attribute.name

            # if there's a key, then address field in json object
            if attribute.key:
                return [f"{alias}.{attribute.name}->>'{attribute.key}' = :{parameter_name}"]

            return [f'{alias}.{attribute.name} = :{parameter_name}']

        if isinstance(attribute, Reference):
            # recurse
            target_alias = attribute.alias or attribute.table
            where_clause = self._nested_attributes(attribute.attributes, target_alias)

            return where_clause

    def _nested_attributes(self, attributes: List[AbstractAttribute], alias):
        return list(chain(*[self._nested_attribute(attribute, alias) for attribute in attributes]))
