"""
This class generates insert statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
import re
from itertools import chain

from stimula.compiler.select_compiler import SelectCompiler


class InsertCompiler:
    """
        header: 'c1[unique=true], c2(c1), c3(b1), c4(b2(a1))'

        query:
        insert into c (c1, c2, c3, c4)
        select :1, c.c0, b.b0, b1.b0
        from c, b, b as b0
        join a on b0.b2=a.a0
        where c.c1 = :2 and b.b1 = :3 and a.a1=:4
        returning id;
    """

    def compile(self, mapping):
        insert_clause = InsertClauseCompiler().compile(mapping)
        select_clause = SelectClauseCompiler().compile(mapping)
        from_clause = FromClauseCompiler(True).compile(mapping)
        where_clause = ForeignWhereClauseCompiler(True).compile(mapping)
        returning_clause = ReturningClauseCompiler().compile(mapping)

        # this where clause is also used in update query. Here we need 'where'
        if where_clause:
            where_clause = ' where ' + where_clause

        return f'{insert_clause}{select_clause}{from_clause}{where_clause}{returning_clause}'


class InsertClauseCompiler:
    def compile(self, mapping):
        # table name is first argument
        table = mapping['table']

        # get lists of lists of attributes
        column_lists = [self._column(c) for c in mapping['columns']]

        # flatten list of lists and comma separate
        columns = ', '.join([a for attributes in column_lists for a in attributes])

        return f'insert into {table}({columns})'

    def _column(self, column):
        attributes = self._attributes(column['attributes'])
        return attributes

    def _attributes(self, attributes):
        # skip extensions on base table, because they are not columns. We'll insert them in a separate query
        return [a['name'] for a in attributes if not a.get('foreign-key', {}).get('extension')]


class SelectClauseCompiler:
    def compile(self, mapping):
        # get list of attributes per column
        columns = [self._column(c) for c in mapping['columns']]
        # flatten list of lists
        attributes = [a for attributes in columns for a in attributes]
        # comma separate cells
        return ' select ' + ', '.join(attributes)

    def _column(self, column):
        attributes = self._attributes(column['attributes'])
        return attributes

    def _attributes(self, attributes):
        # iterate attributes to get list of lists.
        # skip extensions on base table, because they are not columns. We'll insert them in a separate query
        return [self._attribute(a) for a in attributes if not a.get('foreign-key', {}).get('extension')]

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            target_name = foreign_key['table']
            target_alias = foreign_key.get('alias', target_name)
            target_column = foreign_key['name']
            # no need to recurse
            return f'{target_alias}.{target_column}'

        # get value from parameter
        return f':{attribute["parameter"]}'


'''
There's a subtle difference in the from clause between insert and update statements with respect to extensions:
insert query: we must not join with extension table, because we're inserting a new record and we don't have a primary key yet.
update query: we must join with extension table, because we're updating an existing record and we need to filter by the extension table.
'''
class FromClauseCompiler:

    def __init__(self, is_insert_query):
        # if this is an insert query, we must not join with extension tables
        self._is_insert_query = is_insert_query

    def compile(self, mapping):
        # get table
        table = mapping['table']

        # compile columns
        columns = [self._column(c, table) for c in mapping['columns']]

        # filter out empty columns that don't need a from clause
        filtered_columns = [column for column in columns if column]
        if not filtered_columns:
            return ''
        return ' from ' + ', '.join(filtered_columns)

    def _column(self, column, table):
        attributes = self._attributes(column['attributes'], table, True)
        return attributes

    def _attributes(self, attributes, source_alias, is_root):
        # assert len(attributes) == 1, f'Can only insert a single attribute per column, found: {attributes}'
        from_clauses = [self._attribute(attribute, source_alias, is_root) for attribute in attributes]
        return ', '.join([c for c in from_clauses if c])

    def _attribute(self, attribute, source_alias, is_root_table):
        # nothing to join if not a foreign key
        if not 'foreign-key' in attribute:
            return None

        # if foreign key is an extension on the root table, then we must not join if it's an insert query
        if is_root_table and attribute['foreign-key'].get('extension') and self._is_insert_query:
            return None

        from_clause = ''

        # if not root, then assume we need a left join. This may require a modifier at some point.
        if not is_root_table:
            from_clause += ' left join '

        # get foreign key
        foreign_key = attribute['foreign-key']

        # add target table
        from_clause += foreign_key['table']

        # add alias
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)
        if target_alias != target_name:
            from_clause += f' as {target_alias}'

        # if not root then add on clause
        if not is_root_table:
            from_clause += f' on {source_alias}.{attribute["name"]} = {target_alias}.{foreign_key["name"]}'

            # if this is an Odoo style extension relation and it's not on the root table, then we need to filter by qualifier (module) and table (model)
            if foreign_key.get('extension'):
                qualifier = foreign_key['qualifier']
                # assume for now that source alias is the table name. This is fine as long as we're not joining the same table multiple times
                table_name = SelectCompiler().get_model_name(source_alias)
                from_clause += f' and {target_alias}.model = \'{table_name}\' and {target_alias}.module = \'{qualifier}\''


        # recurse
        return from_clause + self._attributes(foreign_key['attributes'], target_alias, False)



class ForeignWhereClauseCompiler:

    def __init__(self, is_insert_query):
        # if this is an insert query, we must not join with extension tables
        self._is_insert_query = is_insert_query
        self._aliases = {}

    def compile(self, mapping):
        # glue cells together
        table_name = mapping['table']
        clauses = [self._column(c, table_name) for c in mapping['columns']]

        return ' and '.join(chain(*clauses))

    def _column(self, column, table):
        return self._attributes(column['attributes'], table, True)

    def _attributes(self, attributes, source_alias, is_root):
        return chain(*[self._attribute(attribute, source_alias, is_root) for attribute in attributes])

    def _attribute(self, attribute, alias, is_root_table):
        if not 'foreign-key' in attribute:

            # only add where clauses for joined tables, but do register for alias
            if is_root_table:
                return []
            parameter_name = attribute.get('parameter', f'{attribute["name"]}')
            return [f'{alias}.{attribute["name"]} = :{parameter_name}']

        foreign_key = attribute['foreign-key']
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)

        # don't add extension conditions if this is an insert query
        if is_root_table and foreign_key.get('extension') and self._is_insert_query:
            return []

        # recurse
        where_clause = self._attributes(foreign_key['attributes'], target_alias, False)

        # add extension conditions if this is the root of an update query
        if is_root_table and foreign_key.get('extension') and not self._is_insert_query:
            # assume for now that the alias is the table name. This is fine as long as we're not joining the same table multiple times
            model = alias.replace('_', '.')
            extension_clause = [f"{target_alias}.module = '{foreign_key['qualifier']}'",
                                f"{target_alias}.model = '{model}'"]
            where_clause = chain(where_clause, extension_clause)

        return where_clause


class ReturningClauseCompiler:
    # returns 'returning id' if this mapping contains an extension relation on the root table

    def compile(self, mapping):
        clauses = chain(*[self._column(c) for c in mapping['columns']])
        if not any([c for c in clauses if c]):
            return ''
        return ' returning id'

    def _column(self, column):
        return self._attributes(column.get('attributes', []))

    def _attributes(self, attributes):
        return [self._attribute(attribute) for attribute in attributes]

    def _attribute(self, attribute):
        return 'foreign-key' in attribute and attribute['foreign-key'].get('extension', False)
