"""
This class generates delete statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""
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
        foreign_where_clause = ForeignWhereClauseCompiler().compile(mapping)

        result = f'delete from {table_name}{using_clause}{where_clause}'
        if foreign_where_clause:
            result += ' and ' + foreign_where_clause

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


class WhereClauseCompiler:
    # This compiler create the usual where clause, but adds statements to restrict the 'using' tables
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
        # this compiler is only for the base table, don't recurse foreign keys
        if not 'foreign-key' in attribute:
            column_name = attribute['name']
            parameter_name = attribute.get('parameter', f'{column_name}')
            return f'{alias}.{column_name} = :{parameter_name}'

        foreign_key = attribute['foreign-key']
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)
        target_column = foreign_key['name']
        return f'{alias}.{attribute["name"]} = {target_alias}.{target_column}'


class ForeignWhereClauseCompiler:

    def __init__(self):
        self._aliases = {}

    def compile(self, mapping):
        # glue cells together
        table_name = mapping['table']
        clauses = [self._column(c, table_name) for c in mapping['columns']]

        # filter and flatten
        filtered_clauses = [clause for cell in clauses for clause in cell if clause]
        if not filtered_clauses:
            return ''
        return ' and '.join(filtered_clauses)

    def _column(self, column, table):
        # no need to list non-unique columns as 'using' table
        if not column.get('unique', False):
            return []

        where_clause_columns = self._attributes(column['attributes'], table, True)
        return [c for c in where_clause_columns if c]

    def _attributes(self, attributes, source_alias, is_root):
        return [self._attribute(attribute, source_alias, is_root) for attribute in attributes]

    def _attribute(self, attribute, alias, is_root_table):
        if not 'foreign-key' in attribute:

            # only add where clauses for joined tables, but do register for alias
            if is_root_table:
                return ''
            parameter_name = attribute.get('parameter', f'{attribute["name"]}')
            return f'{alias}.{attribute["name"]} = :{parameter_name}'

        foreign_key = attribute['foreign-key']
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)

        # recurse
        return ' and '.join(self._attributes(foreign_key['attributes'], target_alias, False))
