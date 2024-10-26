"""
This class generates delete statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from itertools import chain

from stimula.compiler.foreign_where_compiler import ForeignWhereClauseCompiler
from stimula.compiler.insert_compiler import ReturningClauseCompiler
from stimula.compiler.where_compiler import WhereClauseCompiler


class DeleteCompiler:
    """
        delete from c
        using c
        left join b on c.c0 = b.b0
        where c.c1 = :c1 and b.b1 = :b1;
    """

    def compile(self, mapping):
        table_name = mapping['table']
        using_clause = UsingClauseCompiler().compile(mapping)
        where_clause = WhereClauseCompiler().compile(mapping)
        foreign_where_clause = ForeignWhereClauseCompiler(False, True).compile(mapping)
        # in a delete statement, the foreign where clause always needs an 'and'
        foreign_where_clause = ' and ' + foreign_where_clause if foreign_where_clause else ''
        # returning clause is needed if we need to delete from an extension table
        returning_clause = ReturningClauseCompiler().compile(mapping)

        result = f'delete from {table_name}{using_clause}{where_clause}{foreign_where_clause}{returning_clause}'

        return result


class UsingClauseCompiler:
    # this class is similar to FromClauseCompiler, but it only uses unique columns
    def compile(self, mapping):
        # get table
        table_name = mapping['table']

        clause_lists = [self._column(c, table_name) for c in mapping['columns']]
        clauses = [clause for clause_list in clause_lists for clause in clause_list]

        if not clauses:
            return ''
        return f' using ' + ', '.join(clauses)

    def _column(self, column, table_name):
        # no need to list non-unique columns as 'using' table
        if not column.get('unique', False):
            return []

        return self._attributes(column['attributes'], table_name, True)

    def _attributes(self, attributes, source_alias, is_root):
        # get attributes that contains a foreign key
        foreign_key_attributes = [a for a in attributes if 'foreign-key' in a]

        return [self._attribute(attribute, source_alias, is_root) for attribute in foreign_key_attributes]

    def _attribute(self, attribute, source_alias, is_root_table):

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

        # recurse
        return from_clause + ''.join(self._attributes(foreign_key['attributes'], target_alias, False))


