"""
This class compiles a mapping into a csv-style file header format

Author: Romke Jonker
Email: romke@rnadesign.net
"""

class HeaderCompiler:
    # attributes to include as UI directives
    UI_ATTRIBUTES = ['enabled', 'primary-key', 'unique', 'in-use', 'default']
    # attributes to exclude from the modifiers list
    NON_MODIFIER_ATTRIBUTES = ['attributes', 'enabled', 'primary-key', 'foreign-key', 'in-use', 'default']

    def compile_csv(self, mapping):
        # only return enabled columns
        if 'columns' not in mapping:
            return ''
        return ', '.join([self._column(c) for c in mapping['columns'] if c.get('enabled', False)])

    def compile_csv_unique(self, mapping):
        # only return unique columns
        if 'columns' not in mapping:
            return ''
        return ', '.join([self._column(c) for c in mapping['columns'] if c.get('unique', False)])

    def compile_list(self, mapping, enabled=False, include_skip=False):
        # if enabled, only return enabled columns.
        # if include_skip, then include skip columns. This is useful when reading from CSV
        if 'columns' not in mapping:
            return []
        return [self._column(c) for c in mapping['columns'] if (not enabled or c.get('enabled', False)) and (include_skip or not c.get('skip'))]

    def compile_list_unique(self, mapping):
        if 'columns' not in mapping:
            return []
        return [self._column(c) for c in mapping['columns'] if c.get('unique', False)]

    def compile_list_non_unique(self, mapping):
        # return list of non-unique columns
        if 'columns' not in mapping:
            return []
        # remove unique columns, also remove 'skip' columns because we don't want to write them to DB
        columns = [self._column(c) for c in mapping['columns'] if not c.get('unique', False) and not c.get('skip', False)]
        # remove empty columns
        return [c for c in columns if c]

    def _column(self, column):
        # empty column does not have attributes
        if 'attributes' not in column:
            return ''
        attributes, _ = self._attributes(column['attributes'])
        modifiers = self._modifiers(column)
        return attributes + modifiers

    def _attributes(self, attributes):
        keys, types = zip(*[self._attribute(a) for a in attributes])
        type = types[0] if len(set(types)) == 1 else None
        return ':'.join(keys), type

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            key, type = self._foreign_key(attribute['foreign-key'])
            return attribute['name'] + key, type

        type = attribute.get('type', None)
        return attribute['name'], type

    def _foreign_key(self, foreign_key):
        key, type = self._attributes(foreign_key['attributes'])
        return f"({key})", type

    def _modifiers(self, column):
        # use an exclusion list, so we can add more attributes later
        modifiers = [self._modifier(key, column[key]) for key in sorted(column.keys()) if key not in self.NON_MODIFIER_ATTRIBUTES]
        if not modifiers:
            return ''
        return '[' + ': '.join(modifiers) + ']'

    def _modifier(self, key, value):
        # if type is not yet string, then convert to string and lowercase, so we get 'true' and not 'True'
        if not isinstance(value, str):
            value = str(value).lower()

        # need to quote if value has any of the following: ' "\'='
        if any(c in value for c in [' ', '"', "'", '=']):
            return f'{key}="{value}"'
        return f'{key}={value}'

    def compile_json(self, mapping):
        result = {
            'table-name': mapping['table']
        }
        # compile columns into json
        if 'columns' in mapping:
            result['columns'] = [self._column_json(c) for c in mapping['columns']]
        return result

    def _column_json(self, column):
        key_without_modifiers, type = self._attributes(column['attributes'])
        modifiers = self._modifiers_json(column)

        # add modifiers to key
        key = key_without_modifiers + self._modifiers(column)

        if not type:
            return {'key': key, **modifiers}
        return {'key': key, 'type': type, **modifiers}

    def _modifiers_json(self, column):
        # copy column attributes that are UI directives
        modifiers = {key: column[key] for key in sorted(column.keys()) if key in self.UI_ATTRIBUTES}
        # foreign-key is a special case, check if any of the attributes have foreign keys
        if [a for a in column['attributes'] if 'foreign-key' in a]:
            modifiers['foreign-key'] = True

        return modifiers
