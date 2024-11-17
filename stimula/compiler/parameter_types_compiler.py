"""
This class compiles a mapping into a dictionary of parameter types for each parameter.

Author: Romke Jonker
Email: romke@stml.io
"""
from itertools import chain


class ParameterTypesCompiler:

    def compile(self, mapping):
        if 'columns' not in mapping:
            return {}
        # compile columns into a list of dictionaries
        columns = chain(*[self._column(c) for c in mapping['columns']])

        # combine the dictionaries into one
        return {c[0]: c[1] for c in columns}

    def _column(self, column):
        return self._attributes(column['attributes'])

    def _attributes(self, attributes):
        return chain(*[self._attribute(a) for a in attributes])

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            return self._attributes(foreign_key['attributes'])

        return [(attribute['parameter'], attribute['type'])]
