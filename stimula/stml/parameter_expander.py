'''
This class expands placeholders in a mapping. A placeholder may expand into multiple values.
The result of expansion is therefore a list of mappings, one for each combination of values.

author: Romke Jonker
email: romke@stml.io
'''
import copy
import itertools
import re
from itertools import chain

from stimula.stml.model import Entity, Attribute, Reference


class ParameterExpander:

    def expand(self, mapping: Entity, substitutions: dict) -> list[Entity]:

        # get placeholders
        placeholders = self._get_placeholders(mapping)

        # if there are no placeholders, return the mapping as is
        if not placeholders:
            return [mapping]

        # get values for all placeholders
        values = {p: list(substitutions.get(p, {}).keys()) for p in placeholders}

        # verify that each placeholder has at least one value
        empty_placeholders = [p for p, v in values.items() if not v]
        if empty_placeholders:
            raise ValueError(f'No value found for placeholders: {empty_placeholders}')

        # create cartesian product of values
        values_product = list(itertools.product(*values.values()))

        # create a list of dicts mapping placeholders to values
        value_maps = [{p: v for p, v in zip(values.keys(), value)} for value in values_product]

        # expand the mapping for each value map
        mappings = [self._expand_values(mapping, value_map) for value_map in value_maps]

        return mappings

    def _get_placeholders(self, mapping):
        # regex to map ${...} placeholders
        regex = r'\$\{([^\}]+)\}'

        #         get list of attributes that can contain a placeholder
        values = list(chain(*[[a.default_value, a.exp, a.key] for a in mapping.attributes if isinstance(a, Attribute)]))
        values.extend(chain(*[[a.default_value, a.exp] for a in mapping.attributes if isinstance(a, Reference)]))

        # find all placeholders in the values
        placeholders = []
        for value in values:
            if value is not None:
                placeholders.extend(re.findall(regex, value))

        return placeholders

    def _expand_values(self, mapping, value_map):
        # create a deep copy of the mapping
        mapping_copy = copy.deepcopy(mapping)

        for a in mapping_copy.attributes:
            if isinstance(a, Attribute):
                a.default_value = self._replace_placeholders(a.default_value, value_map)
                a.exp = self._replace_placeholders(a.exp, value_map)
                a.key = self._replace_placeholders(a.key, value_map)
            if isinstance(a, Reference):
                a.default_value = self._replace_placeholders(a.default_value, value_map)
                a.exp = self._replace_placeholders(a.exp, value_map)

        return mapping_copy

    def _replace_placeholders(self, value, value_map):
        for k, v in value_map.items():
            if value is not None:
                value = value.replace(f'${{{k}}}', v)
        return value

#
