"""
This class adds alias names to tables and parameter names to terminal (not foreign key) columns.
The reason we shouldn't create parameter names on the fly when generating SQL queries, is that the order in
which the sql compilers process cells is hard to predict. For example, UpdateCompiler leaves parsing
of unique columns to the end (when generating the where clause). This makes it particularly hard to
create column headers with the correct parameter names.
This compiler generates parameter names in a single linear parsing round.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import copy


class AliasCompiler:
    def __init__(self):
        self._table_suffixes = {}
        self._parameter_suffixes = {}

    def compile(self, mapping):
        # must not re-use this instance
        assert not self._table_suffixes, 'AliasCompiler instance must not be re-used'
        # create deep copy of mapping to avoid modifying the original
        aliased_mapping = copy.deepcopy(mapping)
        table_name = aliased_mapping['table']
        self._table_suffixes[table_name] = 0
        [self._column(c) for c in aliased_mapping['columns']]
        return aliased_mapping

    def _column(self, column):
        # column may be empty
        if column:
            attributes = column['attributes']
            self._attributes(attributes)

    def _attributes(self, attributes):
        [self._attribute(attribute) for attribute in attributes]

    def _attribute(self, attribute):
        if not 'foreign-key' in attribute:
            # create unique parameter name so queries use predictable names
            attribute_name = attribute['name']
            parameter_name = self._get_unique_parameter_id(f'{attribute_name}')
            attribute['parameter'] = parameter_name
        else:
            foreign_key = attribute['foreign-key']
            table_name = foreign_key['table']
            alias = self._get_unique_table_id(table_name)
            # set alias
            foreign_key['alias'] = alias
            self._attributes(foreign_key['attributes'])

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
