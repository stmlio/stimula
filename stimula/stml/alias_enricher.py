"""
This class adds alias names to tables and parameter names to terminal (not foreign key) columns.
The reason we shouldn't create parameter names on the fly when generating SQL queries, is that the order in
which the sql compilers process cells is hard to predict. For example, UpdateCompiler leaves parsing
of unique columns to the end (when generating the where clause). This makes it particularly hard to
create column headers with the correct parameter names.
This compiler generates parameter names in a single linear parsing round.

Author: Romke Jonker
Email: romke@stml.io
"""

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class AliasEnricher:
    def __init__(self):
        self._table_suffixes = {}
        self._parameter_suffixes = {}

    def enrich(self, entity: Entity):
        # must not re-use this instance
        assert not self._table_suffixes, 'AliasEnricher instance must not be re-used'

        # initialize table suffix at 0
        self._table_suffixes[entity.name] = 0

        # iterate all properties
        [self._property(p) for p in entity.attributes]
        return entity

    def _property(self, property: AbstractAttribute):
        if isinstance(property, Attribute):
            # set unique parameter name so queries use predictable names
            property.parameter = self._get_unique_parameter_id(property.name)

        if isinstance(property, Reference):
            # set alias
            property.alias = self._get_unique_table_id(property.table)

            # recurse into properties
            [self._property(p) for p in property.attributes]


    def _get_unique_table_id(self, name):
        if name not in self._table_suffixes:
            self._table_suffixes[name] = 0
            return name
        # increase suffix to make unique
        self._table_suffixes[name] += 1
        return name + '_' + str(self._table_suffixes[name])

    def _get_unique_parameter_id(self, name):
        if name not in self._parameter_suffixes:
            self._parameter_suffixes[name] = 0
            return name
        # increase suffix to make unique
        self._parameter_suffixes[name] += 1
        return name + '_' + str(self._parameter_suffixes[name])
