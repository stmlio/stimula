"""
This class renders a mapping into a csv-style file header format

Author: Romke Jonker
Email: romke@stml.io
"""
from typing import List

from stimula.stml.model import Entity, AbstractAttribute, Reference, Attribute


class HeaderRenderer:
    # attributes to include as UI directives
    UI_ATTRIBUTES = ['enabled', 'primary_key', 'unique', 'in_use', 'default']
    # attributes to exclude from the modifiers list
    NON_MODIFIER_ATTRIBUTES = ['name', 'type', 'table', 'target_name', 'attributes', 'enabled', 'primary_key', 'foreign_key', 'in_use', 'default', 'parameter', 'alias', 'extension', 'deduplicate', 'orm_only', 'qualifier']

    def render_csv(self, mapping: Entity):
        # only return enabled columns
        return ', '.join([self._column(a) for a in mapping.attributes if a.enabled])

    def render_csv_unique(self, mapping: Entity):
        # only return unique columns
        return ', '.join([self._column(a) for a in mapping.attributes if a.unique])

    def render_list(self, mapping: Entity, enabled=False, include_skip=False, include_orm_only=False):
        # if enabled, only return enabled columns.
        # if include_skip, then include skip columns. This is useful when reading from CSV
        return [self._column(a) for a in mapping.attributes if (not a) or ((not enabled or a.enabled) and (include_skip or not a.skip) and (include_orm_only or not a.orm_only))]

    def render_list_unique(self, mapping: Entity):
        # return list of unique columns, skip empty columns
        return [self._column(a) for a in mapping.attributes if a and a.unique]

    def render_list_non_unique(self, mapping: Entity):
        # return list of non-unique columns
        # remove unique columns, also remove 'skip' columns because we don't want to write them to DB. Leave in 'orm-only' columns because we need them to write to ORM.
        # skip empty columns
        return [self._column(a) for a in mapping.attributes if a and (not a.unique) and (not a.skip)]

    def render_list_non_unique_root_extension(self, mapping: Entity):
        # return list of non-unique columns that are root extension columns
        # remove unique columns, also remove 'skip' columns because we don't want to write them to DB
        # skip empty columns
        columns = [self._column(a) for a in mapping.attributes if a and (not a.unique) and (not a.skip) and self._is_root_extension(a)]
        # remove empty columns
        return [c for c in columns if c]

    def _is_root_extension(self, attribute: AbstractAttribute):
        # return true if this is an extension column
        return isinstance(attribute, Reference) and attribute.extension

    def render_list_deduplicate(self, mapping: Entity):
        # return list of attributes to deduplicate. Skip empty columns.
        return [self._column(a) for a in mapping.attributes if a and a.deduplicate]

    def _column(self, attribute: AbstractAttribute):
        # empty column does not have attributes
        if not attribute:
            return ''
        attributes, _ = self._attributes([attribute])
        modifiers = self._modifiers(attribute)
        return attributes + modifiers

    def _attributes(self, attributes: List[AbstractAttribute]):
        keys, types = zip(*[self._attribute(a) for a in attributes])
        type = types[0] if len(set(types)) == 1 else None
        return ':'.join(keys), type

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            return attribute.name, attribute.type

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
