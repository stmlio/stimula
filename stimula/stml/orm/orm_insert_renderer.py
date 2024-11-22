"""
This class generates the query needed to resolved foreign keys for the ORM insert operation.

Author: Romke Jonker
Email: romke@stml.io
"""
from itertools import chain
from typing import List

from stimula.stml.sql.foreign_where_renderer import ForeignWhereClauseRenderer
from stimula.stml.sql.insert_renderer import FromClauseRenderer
from stimula.stml.model import Entity, AbstractAttribute, Attribute, Reference


class OrmInsertRenderer:
    """
        header: 'c1[unique=true], c2(c1), c3(b1), c4(b2(a1))'

        query:
        select c.c0, b.b0, b1.b0
        from c, b, b as b0
        join a on b0.b2=a.a0
        where c.c1 = :2 and b.b1 = :3 and a.a1=:4
    """

    def render(self, mapping: Entity):
        select_clause = SelectClauseRenderer().render(mapping)
        from_clause = FromClauseRenderer(False).render(mapping)
        where_clause = ForeignWhereClauseRenderer(False, False).render(mapping)

        # this where clause is also used in update query. Here we need 'where'
        if where_clause:
            where_clause = ' where ' + where_clause

        return f'{select_clause}{from_clause}{where_clause}'


class SelectClauseRenderer:
    def render(self, mapping: Entity):
        # get list of attributes
        attributes = self._attributes(mapping.attributes)

        # comma separate cells. For an insert, there may be nothing to select
        return 'select ' + ', '.join(attributes) if attributes else ''


    def _attributes(self, attributes: List[AbstractAttribute]):
        # iterate attributes to get list of lists.
        return list(chain(*[self._attribute(a) for a in attributes]))

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            # nothing to select for simple parameters
            return []

        if isinstance(attribute, Reference):
            # table names may need an alias
            # TODO: check if we need to alias the target column
            target_alias = attribute.alias or attribute.table
            # no need to recurse
            return [f'{target_alias}.{attribute.target_name} as {attribute.name}']



class OrmParameterNamesRenderer:
    # returns a list of parameter names that we need to pass to the orm create function, excluding
    # those that come from the query
    def render(self, mapping: Entity):
        # get list of attributes
        return self._attributes(mapping.attributes)

    def _attributes(self, attributes: List[AbstractAttribute]):
        # iterate attributes and flatten
        return list(chain(*[self._attribute(a) for a in attributes]))

    def _attribute(self, attribute: AbstractAttribute):
        if isinstance(attribute, Attribute):
            # return the parameter name, but without the colon
            return [f'{attribute.parameter}']

        if isinstance(attribute, Reference):
            # we'll resolve foreign keys in they query
            return []

