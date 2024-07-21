"""
This class compiles a mapping into an update query.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
from .alias_compiler import AliasCompiler
from .insert_compiler import ForeignWhereClauseCompiler, FromClauseCompiler


class UpdateCompiler:
    """
        update c
        set c2 = c0.c0, c3 = b.b0, c4 = b0.b0
        from c as c0, b, b as b0
        left join a on b0.b2 = a.a0
        where c.c1 = 'c4' and c0.c1 = 'c2' and b.b1 = 'b1' and a.a1 = 'a2';
    """

    def compile(self, mapping):
        aliased_mapping = AliasCompiler().compile(mapping)
        update_clause = UpdateClauseCompiler().compile(aliased_mapping)
        from_clause = FromClauseCompiler(False).compile(aliased_mapping)
        where_clause = WhereClauseCompiler().compile(aliased_mapping)
        foreign_where_clause = ForeignWhereClauseCompiler(False).compile(aliased_mapping)

        result = f'{update_clause}{from_clause}{where_clause}'
        if foreign_where_clause:
            result += ' and ' + foreign_where_clause
        return result


class UpdateClauseCompiler:
    def compile(self, mapping):
        table = mapping['table']

        # compile and filter
        clause_lists = [self._column(c, table) for c in mapping['columns']]
        clauses = [clause for clause_list in clause_lists for clause in clause_list]

        # comma separate cells
        return f'update {table} set ' + ', '.join(clauses)

    def _column(self, column, table):
        unique = column.get('unique', False)
        # can't update unique columns
        if unique:
            return []

        return [self._attribute(attribute) for attribute in column['attributes']]

    def _attribute(self, attribute):
        # if no foreign key, get value from parameter
        if not 'foreign-key' in attribute:
            column_name = attribute['name']
            parameter_name = attribute['parameter']
            return f'{column_name} = :{parameter_name}'
        else:
            foreign_key = attribute['foreign-key']
            column_name = attribute['name']
            # but table names may need an alias
            target_table = foreign_key['table']
            target_alias = foreign_key.get('alias', target_table)
            target_name = foreign_key['name']
            # no need to recurse
            return f'{column_name} = {target_alias}.{target_name}'


class WhereClauseCompiler:
    def compile(self, mapping):
        table_name = mapping['table']
        # filter columns
        clause_lists = [self._column(c, table_name) for c in mapping['columns']]
        clauses = [clause for clause_list in clause_lists for clause in clause_list]

        # must not be empty
        if not clauses:
            raise Exception('Header must have at least one unique column')

        return ' where ' + ' and '.join(clauses)

    def _column(self, column, table_name):
        unique = column.get('unique', False)
        if not unique:
            return []

        return [self._attribute(attribute, table_name) for attribute in column['attributes']]

    def _attribute(self, attribute, alias):
        column_name = attribute['name']
        # this compiler is only for the base table, don't recurse foreign keys

        if 'foreign-key' in attribute:
            # restrict the foreign key to the base table
            foreign_key = attribute['foreign-key']
            return f'{alias}.{column_name} = {foreign_key["alias"]}.{foreign_key["name"]}'

        # if no foreign key, restrict base table to value from parameter
        parameter_name = attribute.get('parameter', f'{column_name}')
        return f'{alias}.{column_name} = :{parameter_name}'
