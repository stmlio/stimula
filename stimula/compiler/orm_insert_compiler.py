"""
This class generates the query needed to resolved foreign keys for the ORM insert operation.

Author: Romke Jonker
Email: romke@stml.io
"""
from itertools import chain

from stimula.compiler.foreign_where_compiler import ForeignWhereClauseCompiler
from stimula.compiler.insert_compiler import FromClauseCompiler


class OrmInsertCompiler:
    """
        header: 'c1[unique=true], c2(c1), c3(b1), c4(b2(a1))'

        query:
        select c.c0, b.b0, b1.b0
        from c, b, b as b0
        join a on b0.b2=a.a0
        where c.c1 = :2 and b.b1 = :3 and a.a1=:4
    """

    def compile(self, mapping):
        select_clause = SelectClauseCompiler().compile(mapping)
        from_clause = FromClauseCompiler(False).compile(mapping)
        where_clause = ForeignWhereClauseCompiler(False, False).compile(mapping)

        # this where clause is also used in update query. Here we need 'where'
        if where_clause:
            where_clause = ' where ' + where_clause

        return f'{select_clause}{from_clause}{where_clause}'


class SelectClauseCompiler:
    def compile(self, mapping):
        # get list of attributes per column
        columns = [self._column(c) for c in mapping['columns']]
        # flatten list of lists
        attributes = [a for attributes in columns for a in attributes]
        # comma separate cells. For an insert, there may be nothing to select
        return 'select ' + ', '.join(attributes) if attributes else ''

    def _column(self, column):
        attributes = self._attributes(column['attributes'])
        return attributes

    def _attributes(self, attributes):
        # iterate attributes to get list of lists.
        return chain(*[self._attribute(a) for a in attributes])

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            foreign_key = attribute['foreign-key']
            # but table names may need an alias
            target_name = foreign_key['table']
            target_alias = foreign_key.get('alias', target_name)
            target_column = foreign_key['name']
            # TODO: check if we need to alias the target column
            target_column_alias = target_column
            # get alias name from attribute.name, that's needed to get to res_id
            attribute_name = attribute['name']
            # no need to recurse
            return [f'{target_alias}.{target_column_alias} as {attribute_name}']

        # nothing to select for simple parameters
        return []


class OrmParameterNamesCompiler:
    # returns a list of parameter names that we need to pass to the orm create function, excluding
    # those that come from the query
    def compile(self, mapping):
        # get list of attributes per column
        return list(chain(*[self._column(c) for c in mapping['columns']]))

    def _column(self, column):
        return self._attributes(column['attributes'])

    def _attributes(self, attributes):
        # iterate attributes and flatten
        return chain(*[self._attribute(a) for a in attributes])

    def _attribute(self, attribute):
        if 'foreign-key' in attribute:
            # we'll resolve foreign keys in they query
            return []

        # return the parameter name, but without the colon
        return [f'{attribute["parameter"]}']
