"""
This class compiles a mapping into a list of parameters for each column.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from itertools import chain
from typing import List

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class ParametersRenderer:

    def render(self, mapping: Entity):
        # Return tuples because they are hashable, so we can use them as key in a dictionary
        return [tuple(self._attribute(a)) for a in mapping.attributes]

    def _attribute(self, attribute: AbstractAttribute) -> List[str]:
        if isinstance(attribute, Attribute):
            return [attribute.parameter]

        if isinstance(attribute, Reference):
            # recurse
            return self._attributes(attribute.attributes)

    def _attributes(self, attributes) -> List[str]:
        return list(chain(*[self._attribute(a) for a in attributes]))
