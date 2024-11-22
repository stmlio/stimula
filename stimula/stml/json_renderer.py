"""
This class renders a mapping into a csv-style file header format

Author: Romke Jonker
Email: romke@stml.io
"""
from typing import List

from stimula.stml.model import AbstractAttribute, Attribute, Reference


class JsonRenderer:
    # attributes to include as UI directives
    UI_ATTRIBUTES = ['enabled', 'primary_key', 'unique', 'in_use', 'default']
    # attributes to exclude from the modifiers list
    NON_MODIFIER_ATTRIBUTES = ['name', 'type', 'table', 'target_name', 'attributes', 'enabled', 'primary_key', 'foreign_key', 'in_use', 'default']

    def render_json(self, mapping):
        result = {
            'table-name': mapping.name
        }
        # compile columns into json
        if mapping.attributes:
            result['columns'] = [self._column_json(a) for a in mapping.attributes]
        return result

    def _column_json(self, attribute: AbstractAttribute):
        key_without_modifiers, type = self._attributes([attribute])
        modifiers = self._modifiers_json(attribute)

        # add modifiers to key
        key = key_without_modifiers + self._modifiers(attribute)

        if not type:
            return {'key': key, **modifiers}
        return {'key': key, 'type': type, **modifiers}

    def _modifiers_json(self, attribute: AbstractAttribute):
        # convert to dict
        attribute_dict = attribute.to_dict()

        # copy column attributes that are UI directives
        keys = [k for k in sorted(attribute_dict.keys()) if k in self.UI_ATTRIBUTES]
        modifiers = {key.replace('_', '-'): attribute_dict[key] for key in keys}

        # foreign-key is a special case, check if the attribute is a foreign keys
        if isinstance(attribute, Reference):
            modifiers['foreign-key'] = True

        return modifiers

    def _attributes(self, attributes: List[AbstractAttribute]):
        keys, types = zip(*[self._attribute(a) for a in attributes])
        type = types[0] if len(set(types)) == 1 else None
        return ':'.join(keys), type

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            return attribute.name, (attribute.type)

        if isinstance(attribute, Reference):
            key, type = self._foreign_key(attribute)
            return attribute.name + key, type

    def _foreign_key(self, foreign_key: Reference):
        key, type = self._attributes(foreign_key.attributes)
        return f"({key})", type

    def _modifiers(self, attribute: AbstractAttribute):
        # convert to dict
        attribute_dict = attribute.to_dict()

        # use an exclusion list, so we can add more attributes later
        modifiers = [self._modifier(key, attribute_dict[key]) for key in sorted(attribute_dict.keys()) if key not in self.NON_MODIFIER_ATTRIBUTES]
        if not modifiers:
            return ''
        return '[' + ': '.join(modifiers) + ']'

    def _modifier(self, key, value):
        # replace '_' with '-' in key
        key = key.replace('_', '-')

        # if type is not yet string, then convert to string and lowercase, so we get 'true' and not 'True'
        if not isinstance(value, str):
            value = str(value).lower()

        # need to quote if value has any of the following: ' "\'='
        if any(c in value for c in [' ', '"', "'", '=']):
            return f'{key}="{value}"'
        return f'{key}={value}'
