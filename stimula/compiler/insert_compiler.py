"""
This class generates insert statements based on a mapping.

Author: Romke Jonker
Email: romke@rnadesign.net
"""


class InsertCompiler:
    """
        header: 'c1[unique=true], c2(c1), c3(b1), c4(b2(a1))'

        query:
        insert into c (c1, c2, c3, c4)
        select :1, c.c0, b.b0, b1.b0
        from c, b, b as b0
        join a on b0.b2=a.a0
        where c.c1 = :2 and b.b1 = :3 and a.a1=:4;
    """

    def compile(self, mapping):
        insert_clause = InsertClauseCompiler().compile(mapping)
        select_clause = SelectClauseCompiler().compile(mapping)
        from_clause = FromClauseCompiler().compile(mapping)
        where_clause = ForeignWhereClauseCompiler().compile(mapping)

        result = f'{insert_clause}{select_clause}{from_clause}'
        if where_clause:
            result += ' where ' + where_clause
        return result


class InsertClauseCompiler:
    def compile(self, mapping):
        # table name is first argument
        table = mapping['table']

        # comma separate columns
        columns = ', '.join([self._column(c) for c in mapping['columns']])

        return f'insert into {table}({columns})'

    def _column(self, column):
        attributes = self._attributes(column['attributes'])
        return attributes

    def _attributes(self, attributes):
        return ', '.join([a['name'] for a in attributes])


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
        # iterate attributes to get list of lists
        return [self._attribute(a) for a in attributes]

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


class FromClauseCompiler:
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
        return from_clause + self._attributes(foreign_key['attributes'], target_alias, False)


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
