'''
Compiles foreign key where clauses for insert and update.
This compiler is used to filter the foreign key tables when inserting or updating records to obtain the foreign key values.
'''

from itertools import chain


class ForeignWhereClauseCompiler:

    def __init__(self, is_insert_query, is_delete_query):
        # if this is an insert query, we must not join with extension tables
        self._is_insert_query = is_insert_query
        self._is_delete_query = is_delete_query
        self._aliases = {}

    def compile(self, mapping):
        # glue cells together
        table_name = mapping['table']
        clauses = [self._column(c, table_name) for c in mapping['columns']]

        return ' and '.join(chain(*clauses))

    def _column(self, column, table):
        # no need to list non-unique columns as 'using' table when creating a delete query
        if self._is_delete_query and not column.get('unique', False):
            return []

        return self._attributes(column['attributes'], table, True, column)

    def _attributes(self, attributes, source_alias, is_root, modifiers):
        return chain(*[self._attribute(attribute, source_alias, is_root, modifiers) for attribute in attributes])

    def _attribute(self, attribute, alias, is_root_table, modifiers):
        if not 'foreign-key' in attribute:

            # no where-clause needed for root table. Only add where clauses for joined tables, but do register for alias
            if is_root_table:
                return []

            # terminate foreign key with an equation
            parameter_name = attribute.get('parameter', f'{attribute["name"]}')

            # if there's a key, then address field in json object
            if 'key' in modifiers:
                return [f"{alias}.{attribute['name']}->>'{modifiers['key']}' = :{parameter_name}"]

            return [f'{alias}.{attribute["name"]} = :{parameter_name}']

        foreign_key = attribute['foreign-key']
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)

        # don't add extension conditions if this is an insert query
        if is_root_table and foreign_key.get('extension') and self._is_insert_query:
            return []

        # recurse
        where_clause = self._attributes(foreign_key['attributes'], target_alias, False, modifiers)

        # add extension conditions if this is the root of an update or delete query
        if is_root_table and foreign_key.get('extension') and foreign_key.get('qualifier') and not self._is_insert_query:
            # assume for now that the alias is the table name. This is fine as long as we're not joining the same table multiple times
            model = alias.replace('_', '.')
            extension_clause = [f"{target_alias}.module = '{foreign_key['qualifier']}'",
                                f"{target_alias}.model = '{model}'"]
            where_clause = chain(where_clause, extension_clause)

        return where_clause
