"""
This class compiles a mapping into a list of parameters for each column.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from itertools import chain


class ParametersCompiler:

    def compile(self, mapping):
        if 'columns' not in mapping:
            return ''
        # list of tuples. Use tuples because they are hashable, so we can use them as keys in a dictionary
        columns = [tuple(self._column(c)) for c in mapping['columns']]
        return columns

    def _column(self, column):
        return self._attributes(column['attributes'])

    def _attributes(self, attributes):
        return chain(*[self._attribute(a) for a in attributes])

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            return self._attributes(foreign_key['attributes'])

        parameter = attribute['parameter']
        return [parameter]
