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

from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class WhereClauseRenderer:

    def __init__(self, is_primary_key_selector=False):
        # if this is a primary key selector, we must not recurse into foreign keys
        self._is_primary_key_selector = is_primary_key_selector

    # This compiler create the usual where clause, but adds statements to restrict the 'using' tables
    def render(self, mapping: Entity):

        # find clauses for unique columns
        clauses = list(chain(*[self._attribute(a, mapping.name) for a in mapping.attributes if a.unique]))

        # must not be empty, except for primary key selector, because that may be restricted by join instead
        if not clauses and not self._is_primary_key_selector:
            raise Exception('Header must have at least one unique column')

        return ' where ' + ' and '.join(clauses)

    def _attribute(self, attribute: AbstractAttribute, alias):
        if isinstance(attribute, Attribute):
            parameter_name = attribute.parameter or attribute.name
            return [f'{alias}.{attribute.name} = :{parameter_name}']

        if self._is_primary_key_selector:
            # a primary key selector uses join...on instead of where...and, so don't create a where clause
            return []

        if isinstance(attribute, Reference):
            # update and delete statements must restrict by foreign keys, no need to recurse
            target_alias = attribute.alias or attribute.table
            return [f'{alias}.{attribute.name} = {target_alias}.{attribute.target_name}']