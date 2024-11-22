"""
This class compiles a mapping into a dictionary of parameter types for each parameter.

Author: Romke Jonker
Email: romke@stml.io
"""
from itertools import chain
from typing import Tuple, List

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class ParameterTypesRenderer:

    def render(self, mapping: Entity):
        # compile columns into a list of dictionaries
        attributes = self._attributes(mapping.attributes)

        # combine the dictionaries into one
        return {a[0]: a[1] for a in attributes}

    def _attributes(self, attributes: List[AbstractAttribute]) -> List[Tuple[str, str]]:
        return list(chain(*[self._attribute(a) for a in attributes]))

    def _attribute(self, attribute: AbstractAttribute) -> List[Tuple[str, str]]:
        if isinstance(attribute, Attribute):
            return [(attribute.parameter, attribute.type)]

        if isinstance(attribute, Reference):
            return self._attributes(attribute.attributes)
