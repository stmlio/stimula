'''
This class compiles a mapping into a where clause that does not recurse into foreign keys. Used for update and delete statements.

There's subtle difference between update/delete statements and the select statement that we need to find the primary key for ORM queries:
- update: update books set ... where ... book.authorid = authors.author_id
- delete: delete from books using authors where ... book.authorid = authors.author_id
- primary key selector: select ... from books join authors on book.authorid = authors.author_id

Author: Romke Jonker
Email: romke@stml.io
'''
from itertools import chain


class WhereClauseCompiler:

    def __init__(self, is_primary_key_selector=False):
        # if this is a primary key selector, we must not recurse into foreign keys
        self._is_primary_key_selector = is_primary_key_selector

    # This compiler create the usual where clause, but adds statements to restrict the 'using' tables
    def compile(self, mapping):
        table_name = mapping['table']
        # filter columns
        clause_lists = [self._column(c, table_name) for c in mapping['columns']]
        clauses = [clause for clause_list in clause_lists for clause in clause_list]

        # must not be empty, except for primary key selector, because that may be restricted by join instead
        if not clauses and not self._is_primary_key_selector:
            raise Exception('Header must have at least one unique column')

        return ' where ' + ' and '.join(clauses)

    def _column(self, column, table_name):
        unique = column.get('unique', False)
        if not unique:
            return []

        return chain(*[self._attribute(attribute, table_name) for attribute in column['attributes']])


    def _attribute(self, attribute, alias):
        # this compiler is only for the base table, don't recurse foreign keys
        column_name = attribute['name']
        if not 'foreign-key' in attribute:
            parameter_name = attribute.get('parameter', f'{column_name}')
            return [f'{alias}.{column_name} = :{parameter_name}']

        if self._is_primary_key_selector:
            # a primary key selector uses join...on instead of where...and, so don't create a where clause
            return []

        # update and delete statements must restrict by foreign keys
        foreign_key = attribute['foreign-key']
        target_name = foreign_key['table']
        target_alias = foreign_key.get('alias', target_name)
        target_column = foreign_key['name']
        return [f'{alias}.{column_name} = {target_alias}.{target_column}']
